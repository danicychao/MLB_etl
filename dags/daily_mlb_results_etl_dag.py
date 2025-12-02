from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

date = "2025-04-21"
main_dir = "/opt/airflow"

DEFAULT_ARGS = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="mlb_etl_results_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 3, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["mlb", "etl"],
)
with dag:

    ## run extract_teams.py to get team information
    extract = BashOperator(
        task_id="extract_games",
        bash_command="""
            set -euo pipefail &&
            echo "extracting today's games..." &&
            rm -r {{ params.main_dir }}/tmp || true &&
            mkdir -p {{ params.main_dir }}/tmp &&
            mkdir -p {{ params.main_dir }}/tmp/raw &&
            python {{ params.main_dir }}/scripts/extract_games.py {{ params.date }} {{ params.main_dir }}/tmp/raw
        """,
        params={
            "date": date,
            "main_dir": main_dir,
        },
        env={
            "PYTHONPATH": main_dir,
        }
    )


    ## get major team information
    transform = BashOperator(
        task_id="get_game_results",
        bash_command="""
            set -euo pipefail &&
            echo "get game results..." &&
            mkdir -p {{ params.main_dir }}/tmp/clean &&
            python {{ params.main_dir }}/scripts/get_game_results.py {{ params.date }} {{ params.main_dir }}/tmp/raw {{ params.main_dir }}/tmp/clean
        """,
        params={
            "date": date,
            "main_dir": main_dir,
        },
        env={
            "PYTHONPATH": main_dir,
        }
    )


    ## load major team information into table
    load = BashOperator(
        task_id="load_results_to_table",
        bash_command="""
            set -euo pipefail &&
            echo "load game results to table..." &&
            python {{ params.main_dir }}/scripts/load_results_to_postgres.py {{ params.date }} {{ params.main_dir }}/tmp/clean
        """,
        params={
            "date": date,
            "main_dir": main_dir,
        },

        env={
            "DATA_DB_HOST": "postgres_data",
            "DATA_DB_PORT": "5432",
            "DATA_DB_USER": "mlb_user",
            "DATA_DB_PASS": "mlb_pass",
            "DATA_DB_NAME": "mlb_db",
            "PYTHONPATH": main_dir,
        }
    )

extract >> transform >> load