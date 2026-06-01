"""
## RAG query pipeline
Queries an existing Pinecone vector store and generates an answer using OpenAI.
1. **Embed** the query via OpenAI
2. **Retrieve** the top-k most relevant chunks from Pinecone
3. **Generate** an answer grounded in the retrieved context
Trigger this DAG manually and supply your question via the `query` parameter.
### Required Airflow variables
`OPENAI_API_KEY`
`PINECONE_API_KEY`
`PINECONE_INDEX_NAME`
"""

import logging

from airflow.models.param import Param
from airflow.sdk import Variable, dag, task
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

    Tries OpenRouter first; falls back to direct OpenAI on auth (401)
    or rate-limit/quota (429) errors.
    model_prefix is "openai/" for OpenRouter, "" for direct OpenAI.
    Uses a minimal embedding call as the probe because models.list()
    is a free metadata endpoint that succeeds even when quota is exhausted.
    """
    from openai import AuthenticationError, OpenAI, RateLimitError

    try:
        client = OpenAI(
            api_key=_get_var("OPENROUTER_API_KEY"),
            base_url=OPENROUTER_BASE_URL,
        )
        # Probing with a 1-token embedding to verify the key works
        client.embeddings.create(input=["ping"], model=f"openai/{EMBEDDING_MODEL}")
        logger.info("Using OpenRouter")
        return client, "openai/"
    except AuthenticationError as exc:
        logger.warning("OpenRouter auth failed (%s), falling back to OpenAI", exc)
    except RateLimitError as exc:
        logger.warning("OpenRouter rate-limited (%s), falling back to OpenAI", exc)
    except Exception as exc:
        logger.warning("OpenRouter unavailable (%s), falling back to OpenAI", exc)
    client = OpenAI(api_key=_get_var("OPENAI_API_KEY"))
    logger.info("Using OpenAI as fallback")
    return client, ""


EMBEDDING_MODEL = "text-embedding-3-small"
CHAT_MODEL = "gpt-4o-mini"


@dag(
    start_date=datetime(2026, 3, 3),
    schedule=None,
    catchup=False,
    doc_md=__doc__,
    default_args={"owner": "data-team", "retries": 2},
    tags=["rag", "ai", "query"],
    params={
        "query": Param(
            default="Where is Casa del Libro?",
            type="string",
            description="The question to ask against the knowledge base.",
        ),
    },
)
def rag_query():
    @task()
    def embed_query(**context) -> list[float]:
        """Embeds the user's query. Tries OpenAI, falls back to OpenRouter."""
        query = context["params"]["query"]
        logger.info("Query: %s", query)
        client, prefix = _get_openai_client()
        model = f"{prefix}{EMBEDDING_MODEL}"
        embedding = client.embeddings.create(input=[query], model=model).data[0].embedding
        logger.info("Query embedded (%d dimensions) via %s", len(embedding), model)
        return embedding

    @task()
    def retrieve_context(query_embedding: list[float]) -> dict:
        """Retrieves most relevant chunks from Pinecone."""
        from pinecone import Pinecone

        pc = Pinecone(api_key=_get_var("PINECONE_API_KEY"))
        index_name = _get_var("PINECONE_INDEX_NAME", "rag-index")
        index = pc.Index(index_name)
        search_results = index.query(vector=query_embedding, top_k=5, include_metadata=True)
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

    @task()
    def generate_answer(retrieval: dict, **context) -> dict:
        """Generates an answer grounded in the retrieved context."""
        query = context["params"]["query"]
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
                    "content": (f"Context:\n{retrieval['context']}\n\nQuestion: {query}"),
                },
            ],
            temperature=0.3,
        )
        answer = response.choices[0].message.content
        result = {
            "query": query,
            "answer": answer,
            "sources": retrieval["sources"],
            "chunks_retrieved": retrieval["chunks_retrieved"],
        }
        logger.info("RAG answer:\n%s", answer)
        logger.info("Sources: %s", retrieval["sources"])
        return result

    embedding = embed_query()
    retrieved = retrieve_context(embedding)
    generate_answer(retrieved)


rag_query()
