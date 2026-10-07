"""DAG untuk sinkronisasi dokumen universitas lainnya dari Scopus."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


JAKARTA_TZ = pendulum.timezone("Asia/Jakarta")
SCRIPT_PATH = "/opt/airflow/scripts/scopus/07_sync_remaining_university_documents.py"
MYSQL_CONN_ID = "db_scopus"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
}


with DAG(
    dag_id="scopus_07_sync_remaining_university_documents",
    description="Sinkronisasi dokumen Scopus untuk universitas lainnya",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 8, 1, tz=JAKARTA_TZ),
    schedule=None,
    is_paused_upon_creation=False,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["scopus", "document", "remaining-university"],
) as dag:
    run_sync_remaining_university_documents = BashOperator(
        task_id="run_sync_remaining_university_documents",
        bash_command=f'test -f "{SCRIPT_PATH}" && python -u "{SCRIPT_PATH}"',
        append_env=True,
        env={
            "PYTHONUNBUFFERED": "1",
            "ELSEVIER_API_KEY": "{{ var.value.ELSEVIER_API_KEY }}",
            "DB_SCOPUS": "db_scopus",
            "DB_HOST": f"{{{{ conn.{MYSQL_CONN_ID}.host }}}}",
            "DB_USER": f"{{{{ conn.{MYSQL_CONN_ID}.login }}}}",
            "DB_PASS": f"{{{{ conn.{MYSQL_CONN_ID}.password }}}}",
        },
        execution_timeout=timedelta(hours=6),
    )
