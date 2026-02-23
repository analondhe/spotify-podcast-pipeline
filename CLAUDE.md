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
- `dags/spotify_podcast_dag.py` — Airflow DAG (`spotify_podcast_pipeline`) scheduled daily at 9 AM, runs Docker Compose services in sequence

## Common Commands

### Docker (primary workflow)
```bash
docker compose run --rm run-all       # Run full pipeline: download → load → dbt run → dbt test
docker compose run --rm download      # Download Kaggle dataset
docker compose run --rm load          # Load CSVs into DuckDB
docker compose run --rm dbt-run       # Run dbt models
docker compose run --rm dbt-test      # Run dbt tests
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
