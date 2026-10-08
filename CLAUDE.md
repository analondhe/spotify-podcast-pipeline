# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

End-to-end data pipeline that downloads Spotify podcast ranking data from Kaggle, loads it into a DuckDB warehouse, and transforms it with dbt. Orchestrated via Docker Compose services or an Airflow DAG.

## Architecture

**Pipeline flow:** Kaggle CSV → `scripts/download_kaggle.py` → `data/` → `scripts/load_to_duckdb.py` → DuckDB (`warehouse/spotify_podcasts.duckdb`, `raw` schema) → dbt models → `analytics` schema

**Key components:**
- `scripts/download_kaggle.py` — Downloads dataset using Kaggle API credentials from env vars
- `scripts/load_to_duckdb.py` — Loads all CSVs from `data/` into DuckDB `raw` schema, auto-sanitizing table names
- `dbt_project/` — dbt project using `dbt-duckdb` adapter; profiles in `dbt_project/profiles.yml` point to `../warehouse/spotify_podcasts.duckdb`
- `dbt_project/models/staging/stg_podcast_episodes.sql` — Main transformation: renames columns from camelCase to snake_case, casts types, computes `duration_minutes`
- `airflow/dags/spotify_podcast_dag.py` — Airflow DAG (`spotify_podcast_pipeline`) scheduled daily at 9 AM, runs Docker Compose services in sequence
- `agent/` — Chat agent over the warehouse (Claude API + tool use). `catalog.py` builds the entity catalog from DuckDB `information_schema` plus dbt YAML descriptions; `tools.py` defines `list_entities`, `describe_entity`, `run_sql`, `render_chart` (read-only, single SELECT, row cap, timeout); `providers.py` holds the model backends (`AnthropicProvider`, `OllamaProvider`; each owns its native message history); `chat.py` is the provider-agnostic loop (`DataAgent.ask` yields events); `cli.py` and `app.py` are the terminal and Streamlit front ends

## Common Commands

### Docker (primary workflow)
```bash
docker compose run --rm run-all       # Run full pipeline: download → load → dbt run → dbt test
docker compose run --rm download      # Download Kaggle dataset
docker compose run --rm load          # Load CSVs into DuckDB
docker compose run --rm dbt-run       # Run dbt models
docker compose run --rm dbt-test      # Run dbt tests
```

### Chat agent
```bash
export ANTHROPIC_API_KEY=...
streamlit run agent/app.py                 # chat UI with charts
python -m agent.cli                        # terminal chat
python -m agent.cli --provider ollama --model qwen3:8b   # local open-source model via Ollama
python -m pytest tests                     # agent tests, no API key needed (fake client)
docker compose run --rm --service-ports chat
```

### Local development (without Docker)
```bash
pip install -r requirements.txt
python scripts/download_kaggle.py
python scripts/load_to_duckdb.py

# dbt commands must run from dbt_project/ with --profiles-dir .
cd dbt_project
dbt deps --profiles-dir .
dbt run --profiles-dir .
dbt test --profiles-dir .
```

## dbt Details

- **Adapter:** dbt-duckdb
- **Profile location:** `dbt_project/profiles.yml` (uses `--profiles-dir .` flag, not `~/.dbt/`)
- **Schemas:** `raw` (source tables from CSV load), `analytics` (dbt-managed models)
- **Tests:** All defined in YAML (`_sources.yml` and `_staging.yml`), no custom SQL or Python tests
- **Packages:** `dbt_utils` (installed via `dbt deps`)

## Environment Requirements

- Python 3.11+
- Docker & Docker Compose
- Kaggle credentials: `KAGGLE_USERNAME` and `KAGGLE_KEY` env vars (loaded from `.env`)
- Chat agent: `PODCAST_AGENT_PROVIDER` (`anthropic` default, or `ollama`); `ANTHROPIC_API_KEY` for Claude; `PODCAST_AGENT_MODEL` (defaults `claude-opus-5-5` / `qwen3:8b`), `PODCAST_AGENT_EFFORT` (Claude, default `medium`), `PODCAST_AGENT_THINK` (Ollama, default on), `OLLAMA_NUM_CTX` (default 16384)
- Model descriptions in `dbt_project/models/**/*.yml` (including `meta.grain` and column descriptions) feed the agent's system prompt; keep them current when changing models
