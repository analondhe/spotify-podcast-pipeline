# Spotify Podcast Pipeline

An end-to-end data pipeline that downloads daily Spotify podcast ranking data, loads it into a DuckDB warehouse, and transforms it through a multi-layer dbt model structure. Orchestrated with Docker Compose and Apache Airflow.

## What This Project Does

Spotify publishes daily top podcast charts across multiple countries. This pipeline ingests that data and enriches it with ISO country codes and World Bank population statistics to answer questions like:

- Which countries have the most diverse podcast charts?
- What is the content mix (audio vs mixed media, explicit vs clean) by region?
- How do podcast charts look when normalized by population?
- Which shows dominate across multiple regions?

## Datasets

| Dataset | Source | Description |
|---------|--------|-------------|
| **Top Spotify Podcasts** | [Kaggle](https://www.kaggle.com/datasets/daniilmiheev/top-spotify-podcasts-daily-updated) | Daily updated podcast rankings across 40+ countries. ~800K rows with episode metadata, show info, rankings, and chart movements. |
| **ISO 3166 Country Codes** | [GitHub (lukes)](https://github.com/lukes/ISO-3166-Countries-with-Regional-Codes) | Country code to name/continent/world region mapping. Used as a dbt seed. |
| **World Bank Population** | [World Bank API](https://data.worldbank.org/indicator/SP.POP.TOTL) | Total population by country (SP.POP.TOTL). Used as a dbt seed for per-capita analysis. |

## Architecture

```
Kaggle CSV ──► download_kaggle.py ──► data/ ──► load_to_duckdb.py ──► DuckDB (raw schema)
                                                                           │
                                                                      dbt models
                                                                           │
                                                     ┌─────────────────────┼──────────────────┐
                                                     ▼                     ▼                   ▼
                                               staging schema     intermediate schema    analytics schema
                                            stg_podcast_episodes  int_region_overview    fct_region_insights
                                                                  int_show_performance   fct_show_insights
                                                                                         fct_publisher_insights
```

### dbt Model Layers

| Layer | Schema | Materialization | Models |
|-------|--------|-----------------|--------|
| **Staging** | `staging` | view | `stg_podcast_episodes` — renames camelCase columns, casts types, computes `duration_minutes` |
| **Intermediate** | `intermediate` | view | `int_region_overview` — enriches episodes with country name, continent, world region, and population |
| | | | `int_show_performance` — scoped view for show-level analysis with population context |
| **Marts** | `analytics` | table | `fct_region_insights` — one row per region per day with aggregated metrics: content mix, listening profile, population context |
| | | | `fct_show_insights` — one row per show: lifetime reach, rank performance, and market size stats |
| | | | `fct_publisher_insights` — one row per publisher: portfolio size, reach, and rank performance |
| **Seeds** | `seeds` | table | `country_codes`, `country_stats`, `country_stats_metadata` |

## Tech Stack

- **Python 3.11** — download and load scripts
- **DuckDB** — local analytical database (warehouse)
- **dbt-core** with **dbt-duckdb** adapter — SQL transformations and data testing
- **Docker & Docker Compose** — containerized pipeline execution
- **Apache Airflow** — DAG-based orchestration (scheduled daily at 9 AM)
- **Kaggle API** — dataset download
- **Claude API** (`anthropic` SDK) or **Ollama** (open-source models) + **Streamlit** + **Plotly** — chat agent over the marts

## Project Structure

```
spotify-podcast-pipeline/
├── airflow/dags/
│   └── spotify_podcast_dag.py        # Airflow DAG (daily schedule)
├── agent/
│   ├── catalog.py                    # Entity catalog from DuckDB + dbt YAML descriptions
│   ├── tools.py                      # list/describe/run_sql/render_chart tools (read-only)
│   ├── charts.py                     # Plotly chart builder
│   ├── providers.py                  # Claude and Ollama backends
│   ├── chat.py                       # Provider-agnostic streaming loop
│   ├── cli.py                        # Terminal chat
│   └── app.py                        # Streamlit chat UI
├── tests/
│   └── test_agent.py                 # Agent tests (no API key needed)
├── scripts/
│   ├── download_kaggle.py            # Downloads dataset from Kaggle
│   └── load_to_duckdb.py            # Loads CSVs into DuckDB raw schema
├── dbt_project/
│   ├── models/
│   │   ├── staging/                  # Source renaming, type casting
│   │   ├── intermediate/             # Enrichment joins (country, population)
│   │   └── marts/                    # Aggregated business-ready tables
│   ├── seeds/                        # Country codes, population data
│   ├── macros/                       # generate_schema_name override
│   ├── dbt_project.yml
│   ├── profiles.yml
│   └── packages.yml
├── Dockerfile
├── docker-compose.yml
├── requirements.txt
└── .env                              # Kaggle credentials (gitignored)
```

## Getting Started

### Prerequisites

- Docker & Docker Compose
- Kaggle account with API credentials

### Setup

1. Clone the repo:
   ```bash
   git clone https://github.com/analondhe/spotify-podcast-pipeline.git
   cd spotify-podcast-pipeline
   ```

2. Create a `.env` file with your Kaggle credentials:
   ```
   KAGGLE_USERNAME=your_username
   KAGGLE_KEY=your_api_key
   ```

3. Run the full pipeline:
   ```bash
   docker compose run --rm run-all
   ```

### Running Individual Steps

```bash
docker compose run --rm download    # Download dataset from Kaggle
docker compose run --rm load        # Load CSVs into DuckDB
docker compose run --rm dbt-run     # Run dbt models
docker compose run --rm dbt-test    # Run dbt tests
```

### Airflow (Optional)

The Airflow DAG runs the same Docker Compose services in sequence on a daily schedule. To use it:

```bash
pip install apache-airflow
export AIRFLOW_HOME=~/airflow
```

Update `dags_folder` in `~/airflow/airflow.cfg` to point to the `airflow/dags/` directory, then start Airflow:

```bash
airflow standalone
```

## Chat With the Warehouse

Once the marts are built you can ask questions in plain language. The agent (in `agent/`) reads the dbt YAML descriptions as its semantic layer, writes read-only DuckDB SQL through tool calls, shows you the SQL and result table for every answer, and renders Plotly charts when you ask for a graph.

It runs on either Claude (Anthropic API) or an open-source model served by [Ollama](https://ollama.com). The provider is a drop-down in the UI or a flag on the CLI.

```bash
pip install -r requirements.txt

# Option A: Claude
export ANTHROPIC_API_KEY=...            # or add it to .env
streamlit run agent/app.py             # chat UI with charts at http://localhost:8501
python -m agent.cli                    # terminal chat (charts saved as HTML under .context/charts/)
python -m agent.cli "Which publishers chart in the most regions?"

# Option B: open-source model, fully local
ollama pull qwen3:8b
python -m agent.cli --provider ollama --model qwen3:8b
PODCAST_AGENT_PROVIDER=ollama streamlit run agent/app.py

docker compose run --rm --service-ports chat   # UI inside Docker (Claude provider)
```

Provider notes:

- **Claude** (`claude-opus-5-5` by default) gives the most reliable SQL and multi-step answers. `PODCAST_AGENT_EFFORT` (default `medium`) tunes depth versus cost.
- **Ollama** needs a model with tool-calling support. Qwen 3 8B works on a 16 GB Mac at roughly 10 to 60 seconds per answer; Gemma 3 does not support tools. Thinking mode is on by default because it noticeably improves multi-step SQL; set `PODCAST_AGENT_THINK=0` for faster, rougher answers. `OLLAMA_NUM_CTX` (default 16384) must stay large enough to hold the data dictionary. Expect weaker answers than Claude on anything needing two or more queries.

Example questions:

- Which shows chart in the most regions, and what is their best rank?
- Compare the audio vs mixed-media share by country as a bar chart.
- How has average episode duration changed over time for Germany and the US?
- Which publishers have the best average rank with at least 3 shows?

How it works:

- `analytics.*` marts are offered first; staging, intermediate, and seed entities are available on request. The `raw` schema is hidden.
- Only single `SELECT` statements run, on a read-only connection, with a row cap and a 30-second timeout.
- Column and grain descriptions come from `dbt_project/models/**/*.yml`, so documenting a model there improves the agent's answers.
- `PODCAST_AGENT_PROVIDER` (`anthropic` or `ollama`) and `PODCAST_AGENT_MODEL` pick the backend; defaults are Claude Opus 5.5 and `qwen3:8b`.
- The agent opens DuckDB read-only, which cannot coexist with a `dbt run` writing the same file. Stop the chat before rebuilding the marts.

Run the agent tests (they use a fake API client, so no key is needed):

```bash
python -m pytest tests
```

## Sample Output

**Top regions by podcast chart diversity (latest snapshot):**

| Country | Shows | Audio % | Mixed % | Avg Duration (min) |
|---------|-------|---------|---------|---------------------|
| Germany | 158 | 50.0 | 50.0 | 53.3 |
| Canada | 157 | 59.0 | 41.0 | 71.9 |
| Netherlands | 156 | 85.5 | 14.5 | 46.0 |

**Shows per million population:**

| Country | Shows | Per Million |
|---------|-------|-------------|
| Ireland | 143 | 26.50 |
| New Zealand | 136 | 25.72 |
| Austria | 145 | 15.80 |
