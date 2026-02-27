from datetime import datetime

from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator

PROJECT_DIR = "/Users/anaghaalondhe/spotify-podcast-pipeline"

default_args = {
    "owner": "airflow",
    "retries": 1,
}

with DAG(
    dag_id="spotify_podcast_pipeline",
    default_args=default_args,
    description="Download, load, and transform Spotify podcast data",
    schedule="0 9 * * *",
    start_date=datetime(2026, 2, 13),
    catchup=False,
    max_active_runs=1,
) as dag:

    download = BashOperator(
        task_id="download",
        bash_command=f"cd {PROJECT_DIR} && docker compose run --rm download",
    )

    load = BashOperator(
        task_id="load",
        bash_command=f"cd {PROJECT_DIR} && docker compose run --rm load",
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"cd {PROJECT_DIR} && docker compose run --rm dbt-run",
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"cd {PROJECT_DIR} && docker compose run --rm dbt-test",
    )

    download >> load >> dbt_run >> dbt_test
