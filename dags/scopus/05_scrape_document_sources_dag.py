"""DAG untuk mengambil document source setiap afiliasi Scopus."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)


JAKARTA_TZ = pendulum.timezone("Asia/Jakarta")
SCRIPT_PATH = "/opt/airflow/scripts/scopus/05_scrape_document_sources.py"
SCOPUS_CONN_ID = "scopus_credentials"
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
    dag_id="scopus_05_scrape_document_sources",
    description="Ambil document source untuk setiap afiliasi Scopus",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 8, 1, tz=JAKARTA_TZ),
    schedule=None,
    is_paused_upon_creation=False,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["scopus", "document-source", "selenium", "t_pt_scopus"],
) as dag:
    run_scrape_document_sources = BashOperator(
        task_id="run_scrape_document_sources",
        bash_command=f'test -f "{SCRIPT_PATH}" && python -u "{SCRIPT_PATH}"',
        append_env=True,
        env={
            "PYTHONUNBUFFERED": "1",
            "SCOPUS_USER": f"{{{{ conn.{SCOPUS_CONN_ID}.login }}}}",
            "SCOPUS_PASS": f"{{{{ conn.{SCOPUS_CONN_ID}.password }}}}",
            "DB_HOST": f"{{{{ conn.{MYSQL_CONN_ID}.host }}}}",
            "DB_USER": f"{{{{ conn.{MYSQL_CONN_ID}.login }}}}",
            "DB_PASS": f"{{{{ conn.{MYSQL_CONN_ID}.password }}}}",
        },
        execution_timeout=timedelta(hours=6),
    )

    trigger_sync_top_20_university_documents = TriggerDagRunOperator(
        task_id="trigger_sync_top_20_university_documents",
        trigger_dag_id="scopus_06_sync_top_20_university_documents",
        trigger_run_id="scopus_06_sync_top_20_university_documents__{{ ts_nodash }}",
        skip_when_already_exists=True,
        wait_for_completion=False,
    )

    run_scrape_document_sources >> trigger_sync_top_20_university_documents
