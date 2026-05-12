# url-rag

Retrieval-Augmented Generation pipeline built with **Apache Airflow**, **Pinecone**, and **OpenAI**.

## What it does

| DAG | Purpose |
|---|---|
| `ingestionrag` | Fetches web pages, chunks them, generates embeddings via OpenAI, and upserts vectors into Pinecone |
| `queryragdag` | Accepts a question as a parameter, embeds it, retrieves the top-k relevant chunks from Pinecone, and generates a grounded answer via OpenAI |

## Quickstart

1. **Set up the environment** — copy `.env.example` to `.env` and fill in your API keys.
2. **Install dependencies**:
   ```bash
   uv sync
   ```
3. **Start Airflow** (via Astronomer):
   ```bash
   astro dev start
   ```
4. **Run the ingestion DAG** from the Airflow UI to populate the vector store.
5. **Trigger the query DAG** with your question in the `query` parameter or use the Click CLI with
   ```bash
uv run url-rag query "What is your real question?"
   ```

## Required Airflow variables

- `OPENAI_API_KEY`
- `PINECONE_API_KEY`
- `PINECONE_INDEX_NAME` (defaults to `rag-index`)
- `rag_source_urls` (optional JSON list; defaults are baked into the DAG)

## Project structure

```
url-rag/
├── dags/
│   ├── ingestionrag.py   # Ingestion pipeline
│   └── queryragdag.py    # Query pipeline
├── include/              # Shared SQL / config (currently empty)
├── plugins/              # Custom Airflow plugins (currently empty)
├── Dockerfile
├── airflow_settings.yaml
├── packages.txt
├── requirements.txt
└── .env.example
```
