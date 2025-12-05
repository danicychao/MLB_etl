from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


main_dir = "/opt/airflow"
raw_dir = f"{main_dir}/tmp/raw"
clean_dir = f"{main_dir}/tmp/clean"

DEFAULT_ARGS = {
    "owner": "you",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    dag_id="mlb_etl_gamedetails_pipeline",
    default_args=DEFAULT_ARGS,
    start_date=datetime(2024, 3, 1),
    schedule_interval=None,
    catchup=False,
    max_active_runs=1,
    tags=["mlb", "etl"],
)
with dag:

    ## run extract_game_details.py to get game details
    extract = BashOperator(
        task_id="extract_plays",
        bash_command="""
            set -euo pipefail &&
            echo "extracting today's plays..." &&
            rm -r {{ params.raw_dir }}/detail || true &&
            mkdir -p {{ params.raw_dir }}/detail &&
            python {{ params.main_dir }}/scripts/extract_game_details.py {{ var.value.game_date }} {{ params.clean_dir }} {{ params.raw_dir }}/detail
        """,
        params={
            "main_dir": main_dir,
            "raw_dir": raw_dir,
            "clean_dir": clean_dir,
        },
        env={
            "PYTHONPATH": main_dir,
        }
    )


    ## get all plays
    transform_play = BashOperator(
        task_id="get_all_plays",
        bash_command="""
            set -euo pipefail &&
            echo "get all plays..." &&
            rm -r {{ params.clean_dir }}/detail || true &&
            mkdir -p {{ params.clean_dir }}/detail &&
            python {{ params.main_dir }}/scripts/get_batter_detail.py {{ var.value.game_date }} {{ params.raw_dir }}/detail {{ params.clean_dir }}/detail
        """,
        params={
            "main_dir": main_dir,
            "raw_dir": raw_dir,
            "clean_dir": clean_dir,
        },
        env={
            "PYTHONPATH": main_dir,
        }
    )


    ## merge teamid to game details
    transform_game_detail = BashOperator(
        task_id="inject_teamid",
        bash_command="""
                set -euo pipefail &&
                echo "get team ids..." &&
                python {{ params.main_dir }}/scripts/merge_batter_team.py {{ var.value.game_date }} {{ params.clean_dir }}/detail
            """,
        params={
            "main_dir": main_dir,
            "clean_dir": clean_dir,
        },
        env={
            "PYTHONPATH": main_dir,
        }
    )

    ## load game details into table
    load = BashOperator(
        task_id="load_gamedetails_to_table",
        bash_command="""
            set -euo pipefail &&
            echo "load game details to table..." &&
            python {{ params.main_dir }}/scripts/load_game_details_to_postgres.py {{ var.value.game_date }} {{ params.clean_dir }}/detail
        """,
        params={
            "main_dir": main_dir,
            "clean_dir": clean_dir,
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

extract >> transform_play >> transform_game_detail >> load