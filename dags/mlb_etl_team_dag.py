from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

year = 2017
main_dir = "/opt/airflow"

DEFAULT_ARGS = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="mlb_etl_team_pipeline",
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
        task_id="extract_teams",
        bash_command="""
            set -euo pipefail &&
            echo "extracting mlb teams..." &&
            rm -r {{ params.main_dir }}/tmp || true &&
            mkdir -p {{ params.main_dir }}/tmp &&
            mkdir -p {{ params.main_dir }}/tmp/raw &&
            python {{ params.main_dir }}/scripts/extract_teams.py {{ params.main_dir }}/tmp/raw --year {{ params.year }}
        """,
        params={
            "year": year,
            "main_dir": main_dir,
        },
    )


    ## get major team information
    transform = BashOperator(
        task_id="get_major_teams",
        bash_command="""
            set -euo pipefail &&
            echo "get major team information..." &&
            mkdir -p {{ params.main_dir }}/tmp/clean &&
            python {{ params.main_dir }}/scripts/get_major_teams.py {{ params.main_dir }}/tmp/raw {{ params.main_dir }}/tmp/clean --year {{ params.year }}
        """,
        params={
            "year": year,
            "main_dir": main_dir,
        },
    )


    ## load major team information into table
    load = BashOperator(
        task_id="load_teams_to_table",
        bash_command="""
            set -euo pipefail &&
            echo "load teams to table..." &&
            python {{ params.main_dir }}/scripts/load_teams_to_postgres.py {{ params.main_dir }}/tmp/clean --year {{ params.year }}
        """,
        params={
            "year": year,
            "main_dir": main_dir,
        },

        env={
            "DATA_DB_HOST": "postgres_data",
            "DATA_DB_PORT": "5432",
            "DATA_DB_USER": "mlb_user",
            "DATA_DB_PASS": "mlb_pass",
            "DATA_DB_NAME": "mlb_db",
        }
    )

extract >> transform >> load