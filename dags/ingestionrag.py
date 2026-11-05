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
`rag_alert_email` and the `smtp_default` connection, for alerts about unexpected failures
URLs loaded from `include/urls.json` (git-ignored) and cleaned at load by
`include/url_filter.py`, so fetches, logs and Pinecone only see cleaned URLs.
"""

import json
import logging
from collections import Counter
from datetime import timedelta

from airflow.sdk import Asset, Variable, dag, task
from pendulum import datetime

from include.url_filter import clean_urls, should_skip_url, vector_ids

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
MAX_DELETE_BATCH = 1000  # to stay within Pinecone's limit on IDs per delete request
ALERT_RECIPIENT_VAR = "rag_alert_email"
ALERT_SMTP_CONN_ID = "smtp_default"
UNEXPECTED_FAILURES_PATH = f"{DATA_DIR}/unexpected_failures.json"

rag_index = Asset("rag_index")


def _write_unexpected(failures: list[dict[str, str]], urls_tried: int, documents_fetched: int) -> None:
    """Saves this run's unexpected failures for the reporting task."""
    report = {"failures": failures, "urls_tried": urls_tried, "documents_fetched": documents_fetched}
    with open(UNEXPECTED_FAILURES_PATH, "w", encoding="utf-8") as fh:
        json.dump(report, fh, ensure_ascii=False)


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

        from include.ingest import FETCH_ERRORS, describe_unexpected, failure_reason, fetch_document

        os.makedirs(DATA_DIR, exist_ok=True)
        # Resetting the report first, so a crash below cannot resend the previous run's failures
        _write_unexpected([], 0, 0)
        urls = clean_urls(json.loads(_get_var("rag_source_urls", _load_default_urls())))
        max_urls = int(_get_var("rag_max_urls", "0"))
        if max_urls > 0:
            logger.info("Limiting to first %d URLs (of %d total)", max_urls, len(urls))
            urls = urls[:max_urls]

        # Pre-filtering
        filtered_urls = []
        skip_reasons: Counter[str] = Counter()
        for url in urls:
            reason = should_skip_url(url)
            if reason:
                logger.debug("Pre-filter skip %s: %s", url, reason)
                skip_reasons[reason.partition(" (")[0]] += 1  # to keep hosts and paths out of the report
            else:
                filtered_urls.append(url)
        pre_skipped = sum(skip_reasons.values())
        logger.info(
            "Pre-filter: %d URLs kept, %d skipped (of %d total): %s",
            len(filtered_urls),
            pre_skipped,
            len(urls),
            dict(skip_reasons.most_common()),
        )

        def _fetch_one(url: str) -> tuple[dict | None, str | None]:
            """Fetches a single URL. Returns the document, or None and the reason it failed."""
            try:
                return fetch_document(url, MAX_CONTENT_CHARS), None
            except FETCH_ERRORS as exc:
                reason = failure_reason(exc)
                logger.warning("Skipping %s: %s (%s)", url, reason, str(exc)[:200])
                return None, reason
            except Exception as exc:  # to keep one page's bug from stopping the batch; it is reported apart
                logger.exception("Unexpected failure on %s", url)
                unexpected.append(describe_unexpected(url, exc))
                return None, "unexpected"

        documents = []
        unexpected: list[dict[str, str]] = []
        fetch_failures: Counter[str] = Counter()
        max_workers = int(_get_var("rag_fetch_workers", "10"))
        logger.info("Fetching %d URLs with %d workers", len(filtered_urls), max_workers)
        with ThreadPoolExecutor(max_workers=max_workers) as pool:
            future_to_url = {pool.submit(_fetch_one, url): url for url in filtered_urls}
            for future in as_completed(future_to_url):
                document, failure = future.result()
                if document is not None:
                    documents.append(document)
                else:
                    fetch_failures[failure] += 1
        total_chars = sum(len(d["content"]) for d in documents)
        logger.info(
            "Fetched %d docs (%d chars total), skipped %d (pre-filter) + %d (fetch errors): %s",
            len(documents),
            total_chars,
            pre_skipped,
            sum(fetch_failures.values()),
            dict(fetch_failures.most_common()),
        )
        _write_unexpected(unexpected, len(filtered_urls), len(documents))
        # Every link failing is an outage, not a list of bad links
        if filtered_urls and not documents:
            raise RuntimeError(f"All {len(filtered_urls)} URLs failed: {dict(fetch_failures.most_common())}")
        # --- CDC: content hashing and change detection ---
        import hashlib

        current_hashes = {}
        for doc in documents:
            doc["content_hash"] = hashlib.sha256(doc["content"].encode()).hexdigest()
            current_hashes[doc["url"]] = doc["content_hash"]
        # Loading previous hash manifest. Only a missing Variable means a first run: any other
        # failure must stop the task, because an empty manifest cannot tell which URLs to purge.
        previous_hashes = json.loads(Variable.get(CONTENT_HASHES_VAR, default="{}"))
        if not previous_hashes:
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
        A batch that keeps failing stops the task: skipping it would purge the
        old vectors, add no new ones, and record the pages as done.
        """
        from include.ingest import with_retries

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
        embedded_chunks = []
        total_batches = (len(chunks) + batch_size - 1) // batch_size

        def _embed_batch(texts: list[str], batch_num: int):
            """Requests one batch of embeddings, rejecting an incomplete response."""
            response = client.embeddings.create(input=texts, model=model)
            if not response.data:
                raise ValueError(f"Empty embedding response for batch {batch_num}")
            if len(response.data) != len(texts):
                raise ValueError(f"Batch {batch_num}: expected {len(texts)} embeddings, got {len(response.data)}")
            return response

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
            response = with_retries(lambda: _embed_batch(texts, batch_num), label=f"Embedding batch {batch_num}")
            for chunk, embedding_data in zip(batch, response.data):
                embedded_chunks.append({**chunk, "embedding": embedding_data.embedding})
        logger.info("Generated embeddings for %d chunks", len(embedded_chunks))
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

        from include.ingest import with_retries

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
        purge_failures = 0
        if urls_to_purge:
            logger.info("Purging vectors for %d changed/removed URLs", len(urls_to_purge))
            for url in urls_to_purge:
                prefix = f"{url}::chunk_"
                ids_to_delete = []
                try:
                    for page in index.list(prefix=prefix):
                        ids_to_delete.extend(vector_ids(page))
                except Exception as exc:
                    purge_failures += 1
                    logger.warning("Failed to list vectors for %s: %s", url[:80], exc)
                    continue
                try:
                    for start in range(0, len(ids_to_delete), MAX_DELETE_BATCH):
                        index.delete(ids=ids_to_delete[start : start + MAX_DELETE_BATCH])
                    total_deleted += len(ids_to_delete)
                    logger.debug("Deleted %d vectors for %s", len(ids_to_delete), url[:80])
                except Exception as exc:
                    purge_failures += 1
                    logger.error("Failed to delete vectors for %s: %s", url[:80], exc)
            logger.info("Deleted %d stale vectors total", total_deleted)
        # Stopping here keeps the previous manifest, the only record of which URLs still need purging
        if purge_failures:
            raise RuntimeError(f"Could not purge {purge_failures} of {len(urls_to_purge)} URLs")
        batch_size = 100
        max_meta_text = 30_000  # to stay safely under Pinecone's 40 KB metadata limit
        total_upserted = 0
        batch_num = 0
        batch: list[dict] = []

        def _flush(vectors: list[dict], bnum: int) -> int:
            """Upserts a batch with retries. A batch that keeps failing stops the task before the manifest is saved."""
            with_retries(lambda: index.upsert(vectors=vectors), label=f"Upsert batch {bnum}")
            return len(vectors)

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
                    total_upserted += _flush(batch, batch_num)
                    logger.info("Upserted batch %d (%d total)", batch_num, total_upserted)
                    batch = []
        # Flushing remaining vectors
        if batch:
            batch_num += 1
            total_upserted += _flush(batch, batch_num)
            logger.info("Upserted batch %d (%d total)", batch_num, total_upserted)
        # --- CDC: persisting the new hash manifest ---
        all_hashes = manifest.get("all_hashes", {})
        if all_hashes:
            Variable.set(CONTENT_HASHES_VAR, json.dumps(all_hashes))
            logger.info("Saved hash manifest (%d entries) to Airflow Variable", len(all_hashes))
        result = {
            "index_name": index_name,
            "chunks_upserted": total_upserted,
            "chunks_deleted": total_deleted,
        }
        logger.info("Upsert complete: %s", result)
        return result

    @task(trigger_rule="all_done")
    def report_unexpected_failures() -> int:
        """Emails one digest of this run's unexpected failures, if there were any.

        This is a parallel branch off fetch_documents on purpose. Airflow derives a
        run's state from its final tasks, so as a step after the upsert, a successful
        email would turn a failed run green. As a second final task, either branch
        failing fails the run: a broken mail setup stays visible without blocking
        the ingest.
        """
        from html import escape

        from airflow.providers.smtp.hooks.smtp import SmtpHook

        from include.ingest import build_alert

        with open(UNEXPECTED_FAILURES_PATH, "r", encoding="utf-8") as fh:
            report = json.load(fh)
        failures = report["failures"]
        if not failures:
            logger.info("No unexpected failures to report")
            return 0
        subject, body = build_alert(failures, report["urls_tried"], report["documents_fetched"])
        with SmtpHook(smtp_conn_id=ALERT_SMTP_CONN_ID) as hook:
            hook.send_email_smtp(
                to=_get_var(ALERT_RECIPIENT_VAR),
                subject=subject,
                html_content=f"<pre>{escape(body)}</pre>",
            )
        logger.info("Sent an alert about %d unexpected failures", len(failures))
        return len(failures)

    fetch_result = fetch_documents()
    chunks_path = chunk_documents(fetch_result["docs_path"])
    embedded_path = generate_embeddings(chunks_path)
    upsert_to_pinecone(embedded_path, fetch_result["manifest_path"])
    fetch_result >> report_unexpected_failures()


rag_ingest()
