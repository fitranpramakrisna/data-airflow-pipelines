"""DAG untuk mengambil dan memperbarui data afiliasi kolaborasi Scopus."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.trigger_dagrun import (
    TriggerDagRunOperator,
)


JAKARTA_TZ = pendulum.timezone("Asia/Jakarta")
SCRAPE_SCRIPT_PATH = "/opt/airflow/scripts/scopus/03_scrape_collaboration_affiliations.py"
COUNTRY_SCRIPT_PATH = "/opt/airflow/scripts/scopus/04_update_collaboration_countries.py"
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
    dag_id="scopus_03_04_collaboration_affiliations",
    description="Ambil data afiliasi kolaborasi lalu perbarui negaranya",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 8, 1, tz=JAKARTA_TZ),
    schedule="0 3 1 * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["scopus", "collaboration", "selenium", "t_pt_scopus"],
) as dag:
    run_scrape_collaboration_affiliations = BashOperator(
        task_id="run_scrape_collaboration_affiliations",
        bash_command=(
            f'test -f "{SCRAPE_SCRIPT_PATH}" '
            f'&& python -u "{SCRAPE_SCRIPT_PATH}"'
        ),
        append_env=True,
        env={
            "PYTHONUNBUFFERED": "1",
            "SCOPUS_USER": f"{{{{ conn.{SCOPUS_CONN_ID}.login }}}}",
            "SCOPUS_PASS": f"{{{{ conn.{SCOPUS_CONN_ID}.password }}}}",
            "ELSEVIER_API_KEY": "{{ var.value.ELSEVIER_API_KEY }}",
            "ELSEVIER_INSTTOKEN": "{{ var.value.ELSEVIER_INSTTOKEN }}",
            "DB_HOST": f"{{{{ conn.{MYSQL_CONN_ID}.host }}}}",
            "DB_USER": f"{{{{ conn.{MYSQL_CONN_ID}.login }}}}",
            "DB_PASS": f"{{{{ conn.{MYSQL_CONN_ID}.password }}}}",
            "DB_AUTH_PLUGIN": (
                "{{ conn.db_scopus.extra_dejson.get("
                "'auth_plugin', 'mysql_native_password') }}"
            ),
        },
        execution_timeout=timedelta(hours=6),
    )

    run_update_collaboration_countries = BashOperator(
        task_id="run_update_collaboration_countries",
        bash_command=(
            f'test -f "{COUNTRY_SCRIPT_PATH}" '
            f'&& python -u "{COUNTRY_SCRIPT_PATH}"'
        ),
        append_env=True,
        env={
            "PYTHONUNBUFFERED": "1",
            "SCOPUS_USER": f"{{{{ conn.{SCOPUS_CONN_ID}.login }}}}",
            "SCOPUS_PASS": f"{{{{ conn.{SCOPUS_CONN_ID}.password }}}}",
            "ELSEVIER_API_KEY": "{{ var.value.ELSEVIER_API_KEY }}",
            "ELSEVIER_INSTTOKEN": "{{ var.value.ELSEVIER_INSTTOKEN }}",
            "DB_HOST": f"{{{{ conn.{MYSQL_CONN_ID}.host }}}}",
            "DB_USER": f"{{{{ conn.{MYSQL_CONN_ID}.login }}}}",
            "DB_PASS": f"{{{{ conn.{MYSQL_CONN_ID}.password }}}}",
            "DB_AUTH_PLUGIN": (
                "{{ conn.db_scopus.extra_dejson.get("
                "'auth_plugin', 'mysql_native_password') }}"
            ),
        },
        execution_timeout=timedelta(hours=6),
    )

    trigger_scrape_document_sources = TriggerDagRunOperator(
        task_id="trigger_scrape_document_sources",
        trigger_dag_id="scopus_05_scrape_document_sources",
        trigger_run_id="scopus_05_scrape_document_sources__{{ ts_nodash }}",
        skip_when_already_exists=True,
        wait_for_completion=False,
    )

    (
        run_scrape_collaboration_affiliations
        >> run_update_collaboration_countries
        >> trigger_scrape_document_sources
    )
