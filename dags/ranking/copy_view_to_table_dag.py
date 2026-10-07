"""DAG domain ranking untuk full refresh tabel Academic Maps."""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.providers.common.sql.operators.sql import (
    SQLCheckOperator,
    SQLExecuteQueryOperator,
)
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from airflow.providers.smtp.operators.smtp import EmailOperator


# Koneksi Airflow harus memiliki akses baca/tulis ke schema ``ranking``.
CONN_ID = "ranking"

AFFILIATION_COLUMNS = """
    affiliation_id,
    affiliation_name,
    country_code,
    affiliation_country,
    city_code,
    affiliation_city
"""

AUTHOR_COLUMNS = """
    id_author,
    author_preferredName,
    author_firstName,
    author_lastName,
    author_orcid,
    author_hindex,
    author_fwciscore
"""

AUTHOR_SOURCE_COLUMNS = """
    id_author,
    author_preferredName,
    author_firstName,
    author_lastName,
    author_orcid,
    NULLIF(TRIM(CAST(author_hindex AS CHAR)), '') AS author_hindex,
    NULLIF(TRIM(CAST(author_fwciscore AS CHAR)), '') AS author_fwciscore
"""

PUBLICATION_COLUMNS = """
    eid,
    id_scopus,
    year,
    date,
    fwci_scopus,
    cited_by
"""

PUBLICATION_AUTHOR_COLUMNS = """
    eid,
    id_author,
    author_sequenced,
    affiliation_id
"""

PUBLICATION_SDG_COLUMNS = """
    eid,
    asjc_category,
    subject_area,
    faculty_area
"""


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}


with DAG(
    dag_id="copy_academic_maps_views_to_tables",
    description="Full refresh lima tabel Academic Maps dari view di schema ranking",
    default_args=default_args,
    start_date=datetime(2025, 1, 1),
    schedule="0 2 15 * *",
    catchup=False,
    max_active_runs=1,
    on_failure_callback=[
        send_smtp_notification(
            smtp_conn_id="push_notification",
            to="{{ var.value.academic_maps_notification_email }}",
            subject="[Airflow][ERROR] DAG {{ dag.dag_id }} gagal",
            html_content="""
                <h3>Proses Academic Maps gagal</h3>
                <p>Terjadi kesalahan saat menjalankan DAG.</p>
                <ul>
                    <li><strong>DAG:</strong> {{ dag.dag_id }}</li>
                    <li><strong>Run ID:</strong> {{ run_id }}</li>
                    <li><strong>Waktu logis:</strong> {{ logical_date }}</li>
                    <li><strong>Error:</strong> {{ exception }}</li>
                </ul>
                <p>Silakan periksa log task terkait di Airflow.</p>
            """,
        )
    ],
    tags=["ranking", "academic-maps", "full-refresh"],
) as dag:
    refresh_academic_maps_tables = SQLExecuteQueryOperator(
        task_id="refresh_academic_maps_tables",
        conn_id=CONN_ID,
        sql=[
            # Hapus tabel anak lebih dahulu agar aman terhadap foreign key.
            "DELETE FROM ranking.t_academic_map_publication_author",
            "DELETE FROM ranking.t_academic_map_publication_sdg",
            "DELETE FROM ranking.t_academic_map_publication",
            "DELETE FROM ranking.t_academic_map_master_author",
            "DELETE FROM ranking.t_academic_map_master_affiliation",
            # Muat tabel master lebih dahulu, kemudian tabel turunannya.
            f"""
                INSERT INTO ranking.t_academic_map_master_affiliation
                    ({AFFILIATION_COLUMNS})
                SELECT {AFFILIATION_COLUMNS}
                FROM ranking.vt_academicMaps_masterAffiliation
            """,
            "SET @academic_maps_original_sql_mode = @@SESSION.sql_mode",
            """
                SET SESSION sql_mode = TRIM(BOTH ',' FROM
                    REPLACE(
                        REPLACE(
                            REPLACE(@@SESSION.sql_mode,
                                'STRICT_TRANS_TABLES', ''),
                            'STRICT_ALL_TABLES', ''),
                        ',,', ','
                    )
                )
            """,
            f"""
                INSERT INTO ranking.t_academic_map_master_author
                    ({AUTHOR_COLUMNS})
                SELECT {AUTHOR_SOURCE_COLUMNS}
                FROM ranking.vt_academicMaps_masterAuthor
            """,
            "SET SESSION sql_mode = @academic_maps_original_sql_mode",
            f"""
                INSERT INTO ranking.t_academic_map_publication
                    ({PUBLICATION_COLUMNS})
                SELECT {PUBLICATION_COLUMNS}
                FROM (
                    SELECT
                        {PUBLICATION_COLUMNS},
                        ROW_NUMBER() OVER (
                            PARTITION BY eid
                            ORDER BY
                                date DESC,
                                cited_by DESC,
                                fwci_scopus DESC,
                                id_scopus DESC
                        ) AS row_num
                    FROM ranking.vt_academicMaps_publication
                ) AS deduplicated_publication
                WHERE row_num = 1
            """,
            f"""
                INSERT INTO ranking.t_academic_map_publication_author
                    ({PUBLICATION_AUTHOR_COLUMNS})
                SELECT {PUBLICATION_AUTHOR_COLUMNS}
                FROM ranking.vt_academicMaps_publicationAuthor
            """,
            f"""
                INSERT INTO ranking.t_academic_map_publication_sdg
                    ({PUBLICATION_SDG_COLUMNS})
                SELECT {PUBLICATION_SDG_COLUMNS}
                FROM ranking.vt_academicMaps_publicationSDG
            """,
        ],
        autocommit=False,
    )

    validate_row_count = SQLCheckOperator(
        task_id="validate_row_count",
        conn_id=CONN_ID,
        sql="""
            SELECT
                EXISTS(
                    SELECT 1
                    FROM ranking.t_academic_map_master_affiliation
                    LIMIT 1
                )
                AND EXISTS(
                    SELECT 1
                    FROM ranking.t_academic_map_master_author
                    LIMIT 1
                )
                AND EXISTS(
                    SELECT 1
                    FROM ranking.t_academic_map_publication
                    LIMIT 1
                )
                AND EXISTS(
                    SELECT 1
                    FROM ranking.t_academic_map_publication_author
                    LIMIT 1
                )
                AND EXISTS(
                    SELECT 1
                    FROM ranking.t_academic_map_publication_sdg
                    LIMIT 1
                ) AS all_destination_tables_have_data
        """,
    )

    send_success_email = EmailOperator(
        task_id="send_success_email",
        conn_id="push_notification",
        to="{{ var.value.academic_maps_notification_email }}",
        subject="[Airflow] Academic Maps berhasil diperbarui",
        html_content="""
            <h3>Proses Academic Maps berhasil</h3>
            <p>Data dari lima view telah disalin dan seluruh tabel tujuan
            berhasil dikonfirmasi terisi.</p>
            <ul>
                <li><strong>DAG:</strong> {{ dag.dag_id }}</li>
                <li><strong>Run ID:</strong> {{ run_id }}</li>
                <li><strong>Waktu logis:</strong> {{ logical_date }}</li>
            </ul>
        """,
    )

    refresh_academic_maps_tables >> validate_row_count >> send_success_email
