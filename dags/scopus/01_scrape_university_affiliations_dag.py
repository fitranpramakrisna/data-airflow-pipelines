"""DAG domain Scopus untuk scraping data perguruan tinggi."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


JAKARTA_TZ = pendulum.timezone("Asia/Jakarta")
SCRIPT_PATH = "/opt/airflow/scripts/scopus/01_scrape_university_affiliations.py"
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
    dag_id="scopus_01_scrape_university_affiliations",
    description="Scrape data afiliasi perguruan tinggi Scopus ke t_pt_scopus_temp",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 8, 1, tz=JAKARTA_TZ),
    schedule="0 1 1 * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["scopus", "scraping", "selenium", "t_pt_scopus"],
) as dag:
    run_scrape_university_affiliations = BashOperator(
        task_id="run_scrape_university_affiliations",
        bash_command=f'test -f "{SCRIPT_PATH}" && python -u "{SCRIPT_PATH}"',
        append_env=True,
        env={
            "PYTHONUNBUFFERED": "1",
            "SCOPUS_USER": f"{{{{ conn.{SCOPUS_CONN_ID}.login }}}}",
            "SCOPUS_PASS": f"{{{{ conn.{SCOPUS_CONN_ID}.password }}}}",
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
