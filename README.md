# url-rag

Retrieval-Augmented Generation pipeline built with **Apache Airflow**, **Pinecone**, and **OpenAI**.

## Sibling dependency: `acidbase`

`url-rag` depends on [`jurdabos/acidbase`](https://github.com/jurdabos/acidbase) — a small, MIT-licensed sibling repo that provides the shared `push` CLI subcommand (and other ecosystem utilities). It is referenced directly via git in `pyproject.toml`:

```toml
dependencies = [
    "acidbase @ git+https://github.com/jurdabos/acidbase.git",
    # ...
]
```

For anyone cloning this repo, the install path is just:

```bash
uv sync
```

Nothing private is required. **As maintainer, I commit to keeping `jurdabos/acidbase` public and installable as long as `url-rag` is published.** If that ever stops being practical, I will inline the necessary pieces here and drop the direct git dependency in the same change. — Blai

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
│   ├── ingestionrag.py        # Ingestion pipeline (fetch → chunk → embed → upsert, with CDC)
│   └── queryragdag.py         # Query pipeline (embed → retrieve → generate)
├── plugins/
│   └── url_helpers_plugin.py  # Registers url_rag.* Jinja macros
├── src/url_rag/
│   ├── cli.py                 # Click CLI (`url-rag query`)
│   ├── rag.py                 # Library-mode RAG pipeline (used by CLI)
│   └── url_filter.py          # Pure URL-skip + content-hash helpers
├── tests/unit/                # pytest unit tests
├── include/                   # Shared SQL / config / URL list (urls.json is gitignored)
├── .github/workflows/lint.yml # ruff + gitleaks CI
├── .gitleaks.toml
├── .pre-commit-config.yaml
├── Dockerfile
├── airflow_settings.yaml
├── packages.txt
├── requirements.txt
├── pyproject.toml
└── .env.example
```

## Development

```bash
uv sync                       # install runtime + dev deps
uv run pytest                 # run the test suite
uv run ruff check .           # lint
uv run ruff format --check .  # format check
pre-commit run --all-files    # run all pre-commit hooks (uv-lock, ruff, gitleaks)
```

CI runs the same `ruff` + `gitleaks` checks on every push and PR (see `.github/workflows/lint.yml`).

## Changelog

See [`CHANGELOG.md`](CHANGELOG.md) — this project follows [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Credits

A substantial portion of the original DAG scaffolding and Pinecone wiring in this project is derived from tutorials by the **The Data and AI Guy** YouTube channel — <https://www.youtube.com/@thedataandaiguy>. Heartfelt thanks for the clear, runnable walk-throughs. Any bugs, design choices, or production hardening (CDC, skip-lists, OpenRouter fallback, CLI) on top of that base are mine.

## License

No license file yet; treat as "all rights reserved" until one is added.
