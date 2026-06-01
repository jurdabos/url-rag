# Changelog
All notable changes to this project will be documented in this file.
The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
## [Unreleased]
### Added
- Canonical a6a CI lint workflow at `.github/workflows/lint.yml` running `ruff check`, `ruff format --check`, and the upstream MIT-licensed `gitleaks` CLI on every push / PR to `main`.
- `.gitleaks.toml` based on the acidbase template, extending the upstream default ruleset and allow-listing `.env.example` placeholder tokens.
- `[tool.ruff]` configuration block in `pyproject.toml` (target `py313`, line-length 120, `select = ["E","W","F","I"]`, `ignore = ["E203"]`, ecosystem-standard per-file overrides for `tests/` and `__init__.py`).
- `[tool.pytest.ini_options]` block in `pyproject.toml` pinning `testpaths = ["tests"]` and `pythonpath = ["src", "plugins"]`.
- `src/url_rag/url_filter.py` — pure, dependency-free `should_skip_url()` and `content_hash()` helpers extracted from the ingestion DAG's skip lists, plus the underlying default tuples (`DEFAULT_SKIP_EXTENSIONS`, `DEFAULT_SKIP_DOMAINS`, `DEFAULT_SKIP_PATH_KEYWORDS`).
- `plugins/url_helpers_plugin.py` — Airflow `AirflowPlugin` that registers the two helpers as Jinja macros under the `url_rag` namespace so DAGs can use them in templated fields.
- `tests/unit/test_url_filter.py` and `tests/unit/test_rag.py` — first pytest coverage for the project: URL filter decision matrix, content-hash invariants, `_get_env` env-var resolver, and Click CLI `--help` smoke tests.
- `README.md`: new sections — "Sibling dependency: `acidbase`" with a maintenance promise, "Development" with the standard uv / pytest / ruff / pre-commit commands, "Changelog" pointer, "Credits" thanking the "The Data and AI Guy" YouTube channel (<https://www.youtube.com/@thedataandaiguy>) for the original DAG scaffolding inspiration, and a "License" placeholder.
- `CHANGELOG.md` (this file).
### Changed
- `.pre-commit-config.yaml` replaced with the canonical a6a template: adds the `astral-sh/ruff-pre-commit` hooks (`ruff --fix`, `ruff-format`), bumps gitleaks to `v8.30.1`, and keeps `uv-lock`.
- `README.md`: clarified development workflow and project structure pointers around the new files.
### Removed
- Stale `.gitignore.bak_20260304_175337` (was tracked even though `*.bak` is in `.gitignore`).
### Notes / clarifications
- The sibling `acidbase` dependency was made public at <https://github.com/jurdabos/acidbase> in advance of flipping `url-rag` to public, so `uv sync` works without any private-repo credentials.
- No real secrets have ever been committed to this repository; `.env` is gitignored and history was re-verified with a manual prefix sweep (`sk-proj-`, `sk-or-`, `pcsk_`, etc.) before going public.
