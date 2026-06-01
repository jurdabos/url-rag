"""
## RAG ingestion pipeline (with CDC)
Builds and **incrementally maintains** the vector store for RAG.
Only documents whose content changed since the last run are re-embedded,
saving OpenAI API credits and Pinecone write units.
1. **Fetch** – Scrapes web URLs, hashes content, detects changes
2. **Chunk** – Splits *changed* documents into overlapping text chunks
3. **Embed** – Generates vector embeddings via OpenAI (changed only)
4. **Store** – Deletes stale vectors, upserts new ones, saves hash manifest
Scheduled to run every 14 days.  Publishes the `rag_index` asset on
completion so downstream DAGs (like `rag_query`) know fresh data is available.
### Required Airflow variables
`OPENAI_API_KEY`
`PINECONE_API_KEY`
`PINECONE_INDEX_NAME`
URLs loaded from `include/urls.json` (git-ignored)
"""

import json
import logging
from datetime import timedelta

from airflow.sdk import Asset, Variable, dag, task
from pendulum import datetime

logger = logging.getLogger(__name__)


OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"


def _get_var(key: str, default: str | None = None) -> str:
    """Gets an Airflow variable, falling back to default if not found.

    Airflow 3 SDK's Variable.get() raises on missing keys instead
    of honouring default_var, so this wraps it with a try/except.
    """
    try:
        return Variable.get(key)
    except Exception:
        if default is not None:
            return default
        raise


def _get_openai_client() -> tuple:
    """Returns (OpenAI client, model_prefix).

    Tries OpenAI first; falls back to OpenRouter on auth (401)
    or rate-limit/quota (429) errors.
    model_prefix is "" for direct OpenAI, "openai/" for OpenRouter.
    Uses a minimal embedding call as the probe because models.list()
    is a free metadata endpoint that succeeds even when quota is exhausted.
    """
    from openai import AuthenticationError, OpenAI, RateLimitError

    try:
        client = OpenAI(api_key=_get_var("OPENAI_API_KEY"))
        # Probing with a 1-token embedding; models.list() does NOT catch quota errors
        client.embeddings.create(input=["ping"], model=EMBEDDING_MODEL)
        logger.info("Using OpenAI directly")
        return client, ""
    except AuthenticationError as exc:
        logger.warning("OpenAI auth failed (%s), falling back to OpenRouter", exc)
    except RateLimitError as exc:
        logger.warning("OpenAI rate-limited/quota exceeded (%s), falling back to OpenRouter", exc)
    except Exception as exc:
        logger.warning("OpenAI unavailable (%s), falling back to OpenRouter", exc)
    client = OpenAI(
        api_key=_get_var("OPENROUTER_API_KEY"),
        base_url=OPENROUTER_BASE_URL,
    )
    logger.info("Using OpenRouter as fallback")
    return client, "openai/"


# Loading URL list from external file (git-ignored; may contain secrets in query strings).
# Falls back to a minimal demo set if the file is missing.
URLS_FILE = "/usr/local/airflow/include/urls.json"


def _load_default_urls() -> str:
    """Loads the URL list from the external JSON file.

    Returns the list as a JSON string (matching the Airflow Variable format).
    Falls back to a small demo list when the file is absent.
    """
    import os

    if os.path.exists(URLS_FILE):
        with open(URLS_FILE, "r", encoding="utf-8") as fh:
            urls = json.load(fh)
        logger.info("Loaded %d URLs from %s", len(urls), URLS_FILE)
        return json.dumps(urls)
    logger.warning("%s not found — using demo URL list", URLS_FILE)
    return json.dumps(
        [
            "https://en.wikipedia.org/wiki/Tuvalu",
            "https://en.wikipedia.org/wiki/Funafuti",
            "https://es.wikipedia.org/wiki/La_novia_gitana_(novela)",
            "https://www.casadellibro.com/regala-agendas-cuadernos",
        ]
    )


CHUNK_SIZE = 1000
CHUNK_OVERLAP = 200
EMBEDDING_MODEL = "text-embedding-3-small"
EMBEDDING_DIMENSION = 1536
MAX_CONTENT_CHARS = 5_000_000
DATA_DIR = "/tmp/rag_pipeline"
CONTENT_HASHES_VAR = "rag_content_hashes"

# URL patterns that won't yield useful scrapeable text
SKIP_EXTENSIONS = (
    ".pdf",
    ".jpg",
    ".jpeg",
    ".png",
    ".svg",
    ".ico",
    ".gif",
    ".mp3",
    ".mp4",
    ".wav",
    ".zip",
    ".gz",
    ".tar",
)
SKIP_DOMAINS = (
    "localhost",
    "youtube.com",
    "music.youtube.com",
    "youtu.be",
    "instagram.com",
    "facebook.com",
    "web.whatsapp.com",
    "drive.google.com",
    "docs.google.com",
    "keep.google.com",
    "mail.google.com",
    "calendar.google.com",
    "classroom.google.com",
    "myactivity.google.com",
    "console.cloud.google.com",
    "maps.google.com",
    "open.spotify.com",
    "listen.tidal.com",
    "soundcloud.com",
    "app.prefect.cloud",
    "app.docusign.com",
    "portal.azure.com",
    "signin.aws.amazon.com",
    "onedrive.live.com",
    "eunorg-my.sharepoint.com",
    "dbc-fd72a54d-4556.cloud.databricks.com",
    "gemini.google.com",
    "claude.ai",
    "chat.openai.com",
    "account.jetbrains.com",
)
SKIP_PATH_KEYWORDS = (
    "/login",
    "/signin",
    "/signup",
    "/register",
    "/profile",
    "/account",
    "/dashboard",
    "/my-collection",
    "/inbox",
)

rag_index = Asset("rag_index")


@dag(
    start_date=datetime(2026, 3, 3),
    schedule=timedelta(days=14),
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "data-team", "retries": 2},
    tags=["rag", "ai", "ingestion", "airflow", "orchestration"],
)
def rag_ingest():
    @task(execution_timeout=timedelta(minutes=10))
    def fetch_documents() -> dict:
        """Fetches web URLs, hashes content, and detects changes via CDC.

        Compares SHA-256 hashes against the previous run's manifest
        (stored in Airflow Variable) and writes only changed documents
        to disk.  Returns paths to the documents file and CDC manifest.
        """
        import os
        from concurrent.futures import ThreadPoolExecutor, as_completed
        from urllib.parse import urlparse

        import requests
        from bs4 import BeautifulSoup

        os.makedirs(DATA_DIR, exist_ok=True)
        urls = json.loads(_get_var("rag_source_urls", _load_default_urls()))
        max_urls = int(_get_var("rag_max_urls", "0"))
        if max_urls > 0:
            logger.info("Limiting to first %d URLs (of %d total)", max_urls, len(urls))
            urls = urls[:max_urls]

        def _should_skip(url: str) -> str | None:
            """Returns a reason string if the URL should be skipped, else None."""
            lower = url.lower()
            if any(lower.endswith(ext) for ext in SKIP_EXTENSIONS):
                return f"file extension ({lower.rsplit('.', 1)[-1]})"
            parsed = urlparse(url)
            hostname = (parsed.hostname or "").lower()
            if any(hostname == d or hostname.endswith(f".{d}") for d in SKIP_DOMAINS):
                return f"skipped domain ({hostname})"
            if hostname.startswith("localhost") or hostname.startswith("127."):
                return "localhost"
            path_lower = parsed.path.lower()
            if any(kw in path_lower for kw in SKIP_PATH_KEYWORDS):
                return f"auth/profile path ({path_lower})"
            # Skipping Google Maps place/dir URLs (huge, no useful text)
            if "google.com/maps" in lower:
                return "Google Maps URL"
            return None

        # Pre-filtering
        filtered_urls = []
        pre_skipped = 0
        for url in urls:
            reason = _should_skip(url)
            if reason:
                logger.debug("Pre-filter skip %s: %s", url, reason)
                pre_skipped += 1
            else:
                filtered_urls.append(url)
        logger.info(
            "Pre-filter: %d URLs kept, %d skipped (of %d total)",
            len(filtered_urls),
            pre_skipped,
            len(urls),
        )

        def _fetch_one(url: str) -> dict | None:
            """Fetches a single URL. Returns doc dict or None on failure."""
            try:
                resp = requests.get(url, timeout=15, headers={"User-Agent": "AirflowRAG/1.0"})
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "html.parser")
                for el in soup(["script", "style", "nav", "footer", "header"]):
                    el.decompose()
                text = soup.get_text(separator="\n", strip=True)
                if not text.strip():
                    logger.warning("Empty content from %s, skipping", url)
                    return None
                if len(text) > MAX_CONTENT_CHARS:
                    logger.info(
                        "Truncating %s from %d to %d chars",
                        url,
                        len(text),
                        MAX_CONTENT_CHARS,
                    )
                    text = text[:MAX_CONTENT_CHARS]
                return {
                    "url": url,
                    "title": (soup.title.string if soup.title else url)[:200],
                    "content": text,
                }
            except Exception as exc:
                logger.warning("Skipping %s: %s", url, str(exc)[:200])
                return None

        documents = []
        fetch_skipped = 0
        max_workers = int(_get_var("rag_fetch_workers", "10"))
        logger.info("Fetching %d URLs with %d workers", len(filtered_urls), max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_to_url = {pool.submit(_fetch_one, url): url for url in filtered_urls}
            for future in as_completed(future_to_url):
                result = future.result()
                if result is not None:
                    documents.append(result)
                else:
                    fetch_skipped += 1
        total_chars = sum(len(d["content"]) for d in documents)
        logger.info(
            "Fetched %d docs (%d chars total), skipped %d (pre-filter) + %d (fetch errors)",
            len(documents),
            total_chars,
            pre_skipped,
            fetch_skipped,
        )
        # --- CDC: content hashing and change detection ---
        import hashlib

        current_hashes = {}
        for doc in documents:
            doc["content_hash"] = hashlib.sha256(doc["content"].encode()).hexdigest()
            current_hashes[doc["url"]] = doc["content_hash"]
        # Loading previous hash manifest
        try:
            previous_hashes = json.loads(Variable.get(CONTENT_HASHES_VAR))
        except Exception:
            previous_hashes = {}
            logger.info("No previous hash manifest found — treating all URLs as new")
        # Partitioning into changed vs. unchanged
        changed_docs = []
        unchanged_count = 0
        for doc in documents:
            if previous_hashes.get(doc["url"]) == doc["content_hash"]:
                unchanged_count += 1
            else:
                changed_docs.append(doc)
        # Detecting removed URLs (in previous manifest but no longer in URL list)
        current_url_set = set(filtered_urls)
        removed_urls = [u for u in previous_hashes if u not in current_url_set]
        # Building new hash map: preserving hashes for fetch-failed URLs still in list
        all_hashes = {u: h for u, h in previous_hashes.items() if u in current_url_set and u not in current_hashes}
        all_hashes.update(current_hashes)
        logger.info(
            "CDC: %d changed, %d unchanged, %d removed, %d failed (hash preserved)",
            len(changed_docs),
            unchanged_count,
            len(removed_urls),
            len(all_hashes) - len(current_hashes),
        )
        # Writing only changed docs to disk
        docs_path = f"{DATA_DIR}/documents.json"
        with open(docs_path, "w", encoding="utf-8") as fh:
            json.dump(changed_docs, fh, ensure_ascii=False)
        # Writing CDC manifest for downstream tasks
        manifest_path = f"{DATA_DIR}/cdc_manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as fh:
            json.dump(
                {
                    "changed_urls": [d["url"] for d in changed_docs],
                    "removed_urls": removed_urls,
                    "all_hashes": all_hashes,
                },
                fh,
                ensure_ascii=False,
            )
        logger.info("Wrote %d changed documents to %s", len(changed_docs), docs_path)
        return {"docs_path": docs_path, "manifest_path": manifest_path}

    @task()
    def chunk_documents(docs_path: str) -> str:
        """Splits docs into smaller overlapping chunks.

        Reads documents from the file at docs_path, writes chunks
        to a new JSON file, and returns that file path.
        """
        with open(docs_path, "r", encoding="utf-8") as fh:
            documents = json.load(fh)
        chunks = []
        for doc in documents:
            text = doc["content"]
            start = 0
            chunk_index = 0
            while start < len(text):
                end = start + CHUNK_SIZE
                chunk_text = text[start:end]
                if end < len(text):
                    last_period = chunk_text.rfind(".")
                    last_newline = chunk_text.rfind("\n")
                    break_point = max(last_period, last_newline)
                    if break_point > CHUNK_SIZE * 0.5:
                        chunk_text = chunk_text[: break_point + 1]
                        end = start + break_point + 1
                chunks.append(
                    {
                        "id": f"{doc['url']}::chunk_{chunk_index}",
                        "text": chunk_text.strip(),
                        "metadata": {
                            "source_url": doc["url"],
                            "title": doc["title"],
                            "chunk_index": chunk_index,
                            "content_hash": doc.get("content_hash", ""),
                        },
                    }
                )
                start = end - CHUNK_OVERLAP
                chunk_index += 1
        logger.info("Created %d chunks from %d documents", len(chunks), len(documents))
        out_path = f"{DATA_DIR}/chunks.json"
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(chunks, fh, ensure_ascii=False)
        return out_path

    @task()
    def generate_embeddings(chunks_path: str) -> str:
        """Generates embeddings for each chunk.

        Reads chunks from the file at chunks_path, writes embedded
        chunks to a new JSON file, and returns that file path.
        Tries OpenAI first; falls back to OpenRouter on auth failure.
        Uses smaller batches for OpenRouter and retries on transient errors.
        """
        import time

        with open(chunks_path, "r", encoding="utf-8") as fh:
            chunks = json.load(fh)
        if not chunks:
            logger.info("No chunks to embed — skipping API call")
            out_path = f"{DATA_DIR}/embedded_chunks.json"
            with open(out_path, "w") as fh:
                json.dump([], fh)
            return out_path
        client, prefix = _get_openai_client()
        model = f"{prefix}{EMBEDDING_MODEL}"
        # Using smaller batches for OpenRouter to avoid empty responses
        is_openrouter = bool(prefix)
        batch_size = 20 if is_openrouter else 100
        max_retries = 3
        embedded_chunks = []
        total_batches = (len(chunks) + batch_size - 1) // batch_size
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i : i + batch_size]
            texts = [chunk["text"] for chunk in batch]
            batch_num = i // batch_size + 1
            logger.info(
                "Embedding batch %d/%d (%d chunks) via %s",
                batch_num,
                total_batches,
                len(texts),
                model,
            )
            # Retrying on transient failures (empty responses, timeouts)
            response = None
            for attempt in range(1, max_retries + 1):
                try:
                    response = client.embeddings.create(input=texts, model=model)
                    if not response.data:
                        raise ValueError(f"Empty embedding response for batch {batch_num}")
                    if len(response.data) != len(texts):
                        raise ValueError(
                            f"Batch {batch_num}: expected {len(texts)} embeddings, got {len(response.data)}"
                        )
                    break
                except Exception as exc:
                    if attempt < max_retries:
                        wait = 2**attempt
                        logger.warning(
                            "Batch %d attempt %d/%d failed (%s), retrying in %ds",
                            batch_num,
                            attempt,
                            max_retries,
                            exc,
                            wait,
                        )
                        time.sleep(wait)
                    else:
                        logger.error(
                            "Batch %d failed after %d attempts (%s), skipping %d chunks",
                            batch_num,
                            max_retries,
                            exc,
                            len(batch),
                        )
                        response = None
            if response and response.data:
                for chunk, embedding_data in zip(batch, response.data):
                    embedded_chunks.append({**chunk, "embedding": embedding_data.embedding})
        skipped = len(chunks) - len(embedded_chunks)
        logger.info(
            "Generated embeddings for %d/%d chunks (%d skipped)",
            len(embedded_chunks),
            len(chunks),
            skipped,
        )
        out_path = f"{DATA_DIR}/embedded_chunks.json"
        with open(out_path, "w", encoding="utf-8") as fh:
            json.dump(embedded_chunks, fh)
        return out_path

    @task(outlets=[rag_index])
    def upsert_to_pinecone(embedded_path: str, manifest_path: str) -> dict:
        """Syncs the Pinecone index with changed content.

        Reads the CDC manifest to delete stale vectors for changed/removed
        URLs, then streams and upserts new embeddings.  Persists the
        content-hash manifest to an Airflow Variable on success.
        """
        import time

        import ijson
        from pinecone import Pinecone, ServerlessSpec

        pc = Pinecone(api_key=_get_var("PINECONE_API_KEY"))
        index_name = _get_var("PINECONE_INDEX_NAME", "rag-index")
        existing_indexes = [idx.name for idx in pc.list_indexes()]
        # Validating existing index dimension; deleting if mismatched
        if index_name in existing_indexes:
            desc = pc.describe_index(index_name)
            if desc.dimension != EMBEDDING_DIMENSION:
                logger.warning(
                    "Index %s has dimension %d, expected %d — deleting and recreating",
                    index_name,
                    desc.dimension,
                    EMBEDDING_DIMENSION,
                )
                pc.delete_index(index_name)
                existing_indexes.remove(index_name)
        if index_name not in existing_indexes:
            logger.info("Creating Pinecone index: %s (dimension=%d)", index_name, EMBEDDING_DIMENSION)
            pc.create_index(
                name=index_name,
                dimension=EMBEDDING_DIMENSION,
                metric="cosine",
                spec=ServerlessSpec(cloud="aws", region="us-east-1"),
            )
            # Waiting for the index to be ready before upserting
            logger.info("Waiting for index %s to become ready", index_name)
            while not pc.describe_index(index_name).status.get("ready", False):
                time.sleep(2)
            logger.info("Index %s is ready", index_name)
        index = pc.Index(index_name)
        # --- CDC: deleting stale vectors for changed/removed URLs ---
        with open(manifest_path, "r", encoding="utf-8") as mfh:
            manifest = json.load(mfh)
        urls_to_purge = manifest.get("changed_urls", []) + manifest.get("removed_urls", [])
        total_deleted = 0
        if urls_to_purge:
            logger.info("Purging vectors for %d changed/removed URLs", len(urls_to_purge))
            for url in urls_to_purge:
                prefix = f"{url}::chunk_"
                ids_to_delete = []
                try:
                    for page in index.list(prefix=prefix):
                        ids_to_delete.extend(page)
                except Exception as exc:
                    logger.warning("Failed to list vectors for %s: %s", url[:80], exc)
                if ids_to_delete:
                    try:
                        index.delete(ids=ids_to_delete)
                        total_deleted += len(ids_to_delete)
                        logger.debug("Deleted %d vectors for %s", len(ids_to_delete), url[:80])
                    except Exception as exc:
                        logger.error("Failed to delete vectors for %s: %s", url[:80], exc)
            logger.info("Deleted %d stale vectors total", total_deleted)
        batch_size = 100
        max_retries = 3
        max_meta_text = 30_000  # to stay safely under Pinecone's 40 KB metadata limit
        total_upserted = 0
        total_skipped = 0
        batch_num = 0
        batch: list[dict] = []

        def _flush(vectors: list[dict], bnum: int) -> int:
            """Upserts a batch with retries. Returns count upserted (0 on failure)."""
            for attempt in range(1, max_retries + 1):
                try:
                    index.upsert(vectors=vectors)
                    return len(vectors)
                except Exception as exc:
                    if attempt < max_retries:
                        wait = 2**attempt
                        logger.warning(
                            "Upsert batch %d attempt %d/%d failed (%s), retrying in %ds",
                            bnum,
                            attempt,
                            max_retries,
                            exc,
                            wait,
                        )
                        time.sleep(wait)
                    else:
                        logger.error(
                            "Upsert batch %d failed after %d attempts (%s), skipping %d vectors",
                            bnum,
                            max_retries,
                            exc,
                            len(vectors),
                        )
                        return 0

        # Streaming JSON array items one by one to keep memory flat
        with open(embedded_path, "rb") as fh:
            for chunk in ijson.items(fh, "item"):
                text = chunk.get("text", "")
                if len(text) > max_meta_text:
                    text = text[:max_meta_text]
                meta = {**chunk.get("metadata", {}), "text": text}
                # ijson parses numbers as Decimal; Pinecone needs native floats
                batch.append(
                    {
                        "id": chunk["id"],
                        "values": [float(v) for v in chunk["embedding"]],
                        "metadata": meta,
                    }
                )
                if len(batch) >= batch_size:
                    batch_num += 1
                    upserted = _flush(batch, batch_num)
                    total_upserted += upserted
                    if upserted == 0:
                        total_skipped += len(batch)
                    logger.info("Upserted batch %d (%d total)", batch_num, total_upserted)
                    batch = []
        # Flushing remaining vectors
        if batch:
            batch_num += 1
            upserted = _flush(batch, batch_num)
            total_upserted += upserted
            if upserted == 0:
                total_skipped += len(batch)
            logger.info("Upserted batch %d (%d total)", batch_num, total_upserted)
        # --- CDC: persisting the new hash manifest ---
        all_hashes = manifest.get("all_hashes", {})
        if all_hashes:
            Variable.set(CONTENT_HASHES_VAR, json.dumps(all_hashes))
            logger.info("Saved hash manifest (%d entries) to Airflow Variable", len(all_hashes))
        result = {
            "index_name": index_name,
            "chunks_upserted": total_upserted,
            "chunks_skipped": total_skipped,
            "chunks_deleted": total_deleted,
        }
        logger.info("Upsert complete: %s", result)
        return result

    fetch_result = fetch_documents()
    chunks_path = chunk_documents(fetch_result["docs_path"])
    embedded_path = generate_embeddings(chunks_path)
    upsert_to_pinecone(embedded_path, fetch_result["manifest_path"])


rag_ingest()
