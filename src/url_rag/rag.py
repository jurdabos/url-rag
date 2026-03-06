"""
Core RAG query pipeline — embed, retrieve, generate.

Loads API keys from environment variables (via .env) so the same
logic can be used from the CLI without Airflow.
"""
import logging
import os

from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
EMBEDDING_MODEL = "text-embedding-3-small"
CHAT_MODEL = "gpt-4o-mini"


def _get_env(key: str, default: str | None = None) -> str:
    """Reads an environment variable, raising if missing and no default."""
    value = os.environ.get(key, default)
    if value is None:
        raise RuntimeError(f"Missing required environment variable: {key}")
    return value


def _get_openai_client() -> tuple:
    """Returns (OpenAI client, model_prefix).

    Tries the OPENAI_API_KEY first; falls back to OPENROUTER_API_KEY
    on authentication or rate-limit errors.
    """
    from openai import AuthenticationError, OpenAI, RateLimitError
    try:
        client = OpenAI(api_key=_get_env("OPENAI_API_KEY"))
        # Probing with a 1-token embedding to catch quota errors
        client.embeddings.create(input=["ping"], model=EMBEDDING_MODEL)
        logger.info("Using OpenAI directly")
        return client, ""
    except (AuthenticationError, RateLimitError) as exc:
        logger.warning("OpenAI unavailable (%s), falling back to OpenRouter", exc)
    except Exception as exc:
        logger.warning("OpenAI unavailable (%s), falling back to OpenRouter", exc)
    client = OpenAI(
        api_key=_get_env("OPENROUTER_API_KEY"),
        base_url=OPENROUTER_BASE_URL,
    )
    logger.info("Using OpenRouter as fallback")
    return client, "openai/"


def embed_query(query: str) -> list[float]:
    """Embeds a query string and returns the vector."""
    client, prefix = _get_openai_client()
    model = f"{prefix}{EMBEDDING_MODEL}"
    embedding = (
        client.embeddings.create(input=[query], model=model)
        .data[0]
        .embedding
    )
    logger.info("Query embedded (%d dimensions) via %s", len(embedding), model)
    return embedding


def retrieve_context(query_embedding: list[float], top_k: int = 5) -> dict:
    """Retrieves the most relevant chunks from Pinecone."""
    from pinecone import Pinecone
    pc = Pinecone(api_key=_get_env("PINECONE_API_KEY"))
    index_name = _get_env("PINECONE_INDEX_NAME", "rag-index")
    index = pc.Index(index_name)
    search_results = index.query(
        vector=query_embedding, top_k=top_k, include_metadata=True
    )
    context_parts = []
    sources = []
    for match in search_results.matches:
        context_parts.append(match.metadata["text"])
        source = match.metadata.get("source_url", "unknown")
        if source not in sources:
            sources.append(source)
    logger.info("Retrieved %d chunks from %d sources", len(context_parts), len(sources))
    return {
        "context": "\n\n---\n\n".join(context_parts),
        "sources": sources,
        "chunks_retrieved": len(context_parts),
    }


def generate_answer(query: str, retrieval: dict) -> dict:
    """Generates an answer grounded in the retrieved context."""
    client, prefix = _get_openai_client()
    model = f"{prefix}{CHAT_MODEL}"
    response = client.chat.completions.create(
        model=model,
        messages=[
            {
                "role": "system",
                "content": (
                    "You are a helpful assistant. Answer the user's query "
                    "based only on the provided context. If the context does not contain "
                    "enough information, say so. Cite your sources."
                ),
            },
            {
                "role": "user",
                "content": f"Context:\n{retrieval['context']}\n\nQuestion: {query}",
            },
        ],
        temperature=0.3,
    )
    answer = response.choices[0].message.content
    return {
        "query": query,
        "answer": answer,
        "sources": retrieval["sources"],
        "chunks_retrieved": retrieval["chunks_retrieved"],
    }


def ask(query: str, top_k: int = 5) -> dict:
    """Runs the full RAG pipeline: embed → retrieve → generate."""
    embedding = embed_query(query)
    retrieval = retrieve_context(embedding, top_k=top_k)
    return generate_answer(query, retrieval)
