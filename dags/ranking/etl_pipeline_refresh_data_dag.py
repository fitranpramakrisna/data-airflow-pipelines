"""DAG untuk menjalankan full refresh staging data Research View."""

from datetime import timedelta

import pendulum
from airflow import DAG
from airflow.providers.standard.operators.bash import BashOperator


JAKARTA_TZ = pendulum.timezone("Asia/Jakarta")
SCRIPT_PATH = "/opt/airflow/scripts/researchiew/etl_pipeline_refresh_data.py"
MYSQL_CONN_ID = "fact_researchview"


default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "email_on_failure": False,
    "email_on_retry": False,
}


with DAG(
    dag_id="etl_pipeline_refresh_data",
    description="Full refresh delapan staging table Research View",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 8, 1, tz=JAKARTA_TZ),
    schedule="0 2 27 * *",
    catchup=False,
    max_active_runs=1,
    max_active_tasks=1,
    tags=["ranking", "research-view", "etl", "full-refresh"],
) as dag:
    run_etl_pipeline_refresh_data = BashOperator(
        task_id="run_etl_pipeline_refresh_data",
        bash_command=f'test -f "{SCRIPT_PATH}" && python -u "{SCRIPT_PATH}"',
        append_env=True,
        env={
            "PYTHONUNBUFFERED": "1",
            "DB_HOST": f"{{{{ conn.{MYSQL_CONN_ID}.host }}}}",
            "DB_PORT": f"{{{{ conn.{MYSQL_CONN_ID}.port or 3306 }}}}",
            "DB_USER": f"{{{{ conn.{MYSQL_CONN_ID}.login }}}}",
            "DB_PASSWORD": f"{{{{ conn.{MYSQL_CONN_ID}.password }}}}",
            "DB_NAME": (
                f"{{{{ conn.{MYSQL_CONN_ID}.schema or 'fact_researchview' }}}}"
            ),
        },
        execution_timeout=timedelta(hours=12),
    )
