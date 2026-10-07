"""DAG domain ranking untuk membangun fact Research View."""

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.empty import EmptyOperator
import pendulum


default_args = {
    "owner": "airflow",
    "start_date": pendulum.datetime(2025, 1, 1, tz="Asia/Jakarta"),
    "retries": 1,
}

with DAG(
    dag_id="fact_researchview_dag",
    default_args=default_args,
    # schedule="0 1 27 * *",  # tanggal 27 setiap bulan pukul 01.00 WIB
    schedule=None,
    catchup=False,
) as dag:
    
    # t_journal_by_subject_area = SQLExecuteQueryOperator(
    #     task_id="t_journal_by_subject_area_update",
    #     conn_id="fact_researchview",
    #     sql="""
    #         INSERT INTO fact_researchview.t_journal_by_subject_area (year, affiliation_id, nama_pt, faculty_area, total_publication)
    #         SELECT
    #             YEAR(tds.tgl_terbit_scopus),
    #             tps.affiliation_id,
    #             MAX(tps.nama_pt),
    #             tdssa.faculty_area,
    #             COUNT(*)
    #         FROM (
    #             SELECT DISTINCT
    #                 ds.eid_scopus,
    #                 ds.affiliation_id,
    #                 ds.tgl_terbit_scopus
    #             FROM db_scopus.t_document_scopus ds
    #             WHERE ds.tgl_terbit_scopus >= '2014-01-01'
    #         ) tds
    #         JOIN db_scopus.t_document_scopus_subject_area tdssa
    #             ON tds.eid_scopus = tdssa.eid_scopus
    #         JOIN db_scopus.t_pt_scopus tps
    #             ON tps.affiliation_id = tds.affiliation_id
    #         GROUP BY 1, 2, 4
    #             ON DUPLICATE KEY UPDATE
    #             total_publication = VALUES(total_publication);
    #  """
    # )
    
    # t_performance_highlights = SQLExecuteQueryOperator(
    #     task_id="t_performance_highlights_update",
    #     conn_id="fact_researchview",
    #     sql="""
    #        INSERT INTO fact_researchview.t_performance_highlights (year, affiliation_id, nama_pt, total_publications, total_citasi)
    #         WITH summary (
    #             year,
    #             affiliation_id,
    #             nama_pt,
    #             total_publications,
    #             total_citasi
    #         ) AS (
    #             SELECT
    #                 LEFT(tds.tgl_terbit_scopus, 4),
    #                 tds.affiliation_id,
    #                 MAX(tps.nama_pt),
    #                 COUNT(tds.eid_scopus),
    #                 SUM(tds.cited_by_scopus)
    #             FROM db_scopus.t_document_scopus tds
    #             JOIN db_scopus.t_pt_scopus tps
    #                 ON tps.affiliation_id = tds.affiliation_id
    #             WHERE tds.tgl_terbit_scopus >= '2014-01-01'
    #             GROUP BY 1, 2
    #         )
    #         SELECT
    #             year,
    #             affiliation_id,
    #             nama_pt,
    #             total_publications,
    #             total_citasi
    #         FROM summary
    #         ON DUPLICATE KEY UPDATE
    #             total_publications = VALUES(total_publications),
    #             total_citasi       = VALUES(total_citasi);

    #  """
    # )

    # t_journal_percentile = SQLExecuteQueryOperator(
    #     task_id="t_journal_percentile_update",
    #     conn_id="fact_researchview",
    #     sql="""
    #         INSERT INTO fact_researchview.t_journal_percentile (year, affiliation_id, nama_pt, percentile_type, total_docs)
    #         SELECT
    #             tcs.rank_year,
    #             tps.affiliation_id,
    #             MAX(tps.nama_pt),
    #             CASE
    #                 WHEN tcs.source_percentile >= 90 AND tcs.source_percentile < 100 THEN 'Top 10%'
    #                 ELSE 'Non Top 10%'
    #             END AS percentile_type,
    #             COUNT(tds.eid_scopus) AS total_docs
    #         FROM db_scopus.t_document_scopus tds
    #         JOIN db_scopus.t_citescore_source tcs
    #             ON tds.source_id = tcs.source_id
    #         JOIN db_scopus.t_pt_scopus tps
    #             ON tds.affiliation_id = tps.affiliation_id
    #         WHERE tcs.rank_year >= 2014
    #         GROUP BY 1, 2, 4
    #         ON DUPLICATE KEY UPDATE
    #         total_docs = VALUES(total_docs);
    #  """
    # )


    # t_journal_quartile = SQLExecuteQueryOperator(
    #     task_id="t_journal_quartile_update",
    #     conn_id="fact_researchview",
    #     sql="""
    #         INSERT INTO fact_researchview.t_journal_quartile
    #         (year, affiliation_id, nama_pt, quartile_type, total_docs)
    #     SELECT
    #         LEFT(tds.tgl_terbit_scopus, 4) AS year,
    #         tps.affiliation_id,
    #         MAX(tps.nama_pt) AS nama_pt,
    #         CASE
    #             WHEN (tds.quartile = '-' OR tds.quartile IS NULL) THEN 'Non-Q'
    #             ELSE tds.quartile
    #         END AS quartile_type,
    #         COUNT(tds.eid_scopus) AS total_docs
    #     FROM db_scopus.t_pt_scopus tps
    #     JOIN db_scopus.t_document_scopus tds
    #         ON tps.affiliation_id = tds.affiliation_id
    #     WHERE tds.tgl_terbit_scopus >= '2014-01-01'
    #     GROUP BY
    #         LEFT(tds.tgl_terbit_scopus, 4),
    #         tps.affiliation_id,
    #         CASE
    #             WHEN (tds.quartile = '-' OR tds.quartile IS NULL) THEN 'Non-Q'
    #             ELSE tds.quartile
    #         END
    #     ON DUPLICATE KEY UPDATE
    #         total_docs = VALUES(total_docs);

    #  """
    # )


    # t_partner_evolution = SQLExecuteQueryOperator(
    #     task_id="t_partner_evolution_update",
    #     conn_id="fact_researchview",
    #     sql="""
    #         INSERT INTO fact_researchview.t_partner_evolution (
    #             year,
    #             affiliation_id,
    #             nama_pt,
    #             total_collaborator_national,
    #             total_collaborator_international
    #         )
    #         SELECT
    #             LEFT(tds.tgl_terbit_scopus, 4) AS year,
    #             tps.affiliation_id,
    #             MAX(tps.nama_pt) AS nama_pt,
    #             COUNT(DISTINCT CASE 
    #                 WHEN LOWER(tucl.country) = 'indonesia' THEN tucl.preferredName
    #             END) AS total_collaboration_national,
    #             COUNT(DISTINCT CASE 
    #                 WHEN LOWER(tucl.country) <> 'indonesia' THEN tucl.preferredName
    #             END) AS total_collaboration_international
    #         FROM db_scopus.t_document_scopus tds
    #         JOIN db_scopus.t_document_scopus_author tdsa
    #             ON tdsa.eid_scopus = tds.eid_scopus
    #         JOIN db_scopus.t_pt_scopus tps
    #             ON tps.affiliation_id = tds.affiliation_id
    #         JOIN db_scopus.t_univ_colaborator_location tucl
    #             ON tucl.affiliation_author_id = tdsa.affiliation_author_id
    #         WHERE
    #             tds.tgl_terbit_scopus >= '2014-01-01'
    #             AND tucl.preferredName <> ''
    #         GROUP BY 1,2
    #         ON DUPLICATE KEY UPDATE
    #         total_collaborator_national = VALUES(total_collaborator_national),
    #         total_collaborator_international = VALUES(total_collaborator_international);


    #  """
    # )


    # t_publication_evolution = SQLExecuteQueryOperator(
    #     task_id="t_publication_evolution_update",
    #     conn_id="fact_researchview",
    #     sql="""
    #         INSERT INTO t_publication_evolution (
    #             year,
    #             affiliation_id,
    #             nama_pt,
    #             total_international_publication,
    #             total_national_publication,
    #             total_internal_publication
    #         )
    #         WITH cte_publication AS (
    #             SELECT
    #                 LEFT(tds.tgl_terbit_scopus, 4) AS year,
    #                 tps.affiliation_id AS affiliation_id,
    #                 MAX(tps.nama_pt) AS nama_pt,
    #                 tdsa.eid_scopus AS eid_scopus,

    #                 CASE
    #                     WHEN SUM(CASE WHEN LOWER(tucl.country) <> 'indonesia' THEN 1 END) >= 1
    #                         THEN 'international'
    #                     WHEN SUM(CASE WHEN LOWER(tucl.country) = 'indonesia'
    #                                 AND tps.affiliation_id <> tucl.affiliation_author_id THEN 1 END) >= 1
    #                         THEN 'national'
    #                     WHEN SUM(CASE WHEN tps.affiliation_id = tucl.affiliation_author_id THEN 1 END) >= 1
    #                         THEN 'internal'
    #                     ELSE 'unknown'
    #                 END AS publication_type

    #             FROM db_scopus.t_document_scopus tds
    #             JOIN db_scopus.t_document_scopus_author tdsa
    #                 ON tdsa.eid_scopus = tds.eid_scopus
    #             JOIN db_scopus.t_pt_scopus tps
    #                 ON tps.affiliation_id = tds.affiliation_id
    #             JOIN db_scopus.t_univ_colaborator_location tucl
    #                 ON tucl.affiliation_author_id = tdsa.affiliation_author_id

    #             WHERE tds.tgl_terbit_scopus >= '2014-01-01'
    #             AND tucl.preferredName <> ''
    #             AND tucl.country <> ''

    #             GROUP BY
    #                 LEFT(tds.tgl_terbit_scopus, 4),
    #                 tps.affiliation_id,
    #                 tdsa.eid_scopus
    #         )

    #         SELECT
    #             cte_publication.year AS year,
    #             cte_publication.affiliation_id AS affiliation_id,
    #             MAX(cte_publication.nama_pt) AS nama_pt,
    #             COUNT(CASE WHEN cte_publication.publication_type = 'international' THEN 1 END)
    #                 AS total_international_publication,
    #             COUNT(CASE WHEN cte_publication.publication_type = 'national' THEN 1 END)
    #                 AS total_national_publication,
    #             COUNT(CASE WHEN cte_publication.publication_type = 'internal' THEN 1 END)
    #                 AS total_internal_publication

    #         FROM cte_publication

    #         GROUP BY
    #             cte_publication.year,
    #             cte_publication.affiliation_id
    #         ON DUPLICATE KEY UPDATE
    #         total_international_publication= VALUES(total_international_publication),
    #         total_national_publication = VALUES(total_national_publication),
    #         total_internal_publication = VALUES(total_internal_publication);
    #  """
    # )

    # t_publication_type = SQLExecuteQueryOperator(
    #     task_id="t_publication_type_update",
    #     conn_id="fact_researchview",
    #     sql="""
    #         INSERT INTO fact_researchview.t_publication_type (year, affiliation_id, nama_pt, type_group, total_docs)
    #             SELECT
    #                 LEFT(tds.tgl_terbit_scopus, 4),
    #                 tds.affiliation_id,
    #                 MAX(tps.nama_pt),
    #                 CASE
    #                     WHEN tds.type_scopus IN ('Journal', 'Trade Journal', 'Book Series', 'Conference Proceeding', 'Report')
    #                         THEN tds.type_scopus
    #                     ELSE 'Unidentified'
    #                 END AS type_group,
    #                 COUNT(tds.eid_scopus) AS total_docs
    #             FROM
    #                 db_scopus.t_document_scopus tds
    #             JOIN
    #                 db_scopus.t_pt_scopus tps ON tps.affiliation_id = tds.affiliation_id
    #             WHERE
    #                 tds.tgl_terbit_scopus >= '2014-01-01'
    #             GROUP BY
    #             1, 2, 4
    #             ON DUPLICATE KEY UPDATE
    #                 total_docs = VALUES(total_docs)
    #  """
    # )


    # t_sdg_mapping = SQLExecuteQueryOperator(
    #     task_id="t_sdg_mapping_update",
    #     conn_id="fact_researchview",
    #     sql="""
    #         INSERT INTO fact_researchview.t_sdg_mapping
    #         (
    #             tahun,
    #             affiliation_id,
    #             nama_pt,
    #             label,
    #             total_dokumen
    #         )
    #         SELECT
    #             LEFT(ds.tgl_terbit_scopus, 4) AS tahun,
    #             ds.affiliation_id,
    #             ucl.preferredName AS nama_pt,
    #             CASE
    #                 WHEN ds.sdg_mapping_scopus IS NULL
    #                     OR ds.sdg_mapping_scopus = ''
    #                     OR ds.sdg_mapping_scopus = '0'
    #                 THEN 'Non-SDG Mapping'
    #                 ELSE 'SDG Mapping'
    #             END AS label,
    #             COUNT(DISTINCT ds.eid_scopus) AS total_dokumen
    #         FROM db_scopus.t_document_scopus ds
    #         JOIN db_scopus.t_univ_colaborator_location ucl
    #             ON ds.affiliation_id = ucl.affiliation_author_id
    #         WHERE ds.tgl_terbit_scopus >= '2014-01-01'
    #         GROUP BY 1, 2, 3, 4
    #         ON DUPLICATE KEY UPDATE
    #         total_dokumen = VALUES(total_dokumen)
    #     """,
    # )

    start_task = EmptyOperator(
        task_id='start_task'
    )

    t_top10_international_countries = SQLExecuteQueryOperator(
        task_id="t_top10_international_countries_update",
        conn_id="fact_researchview",
        sql="""
            INSERT INTO fact_researchview.airflow_testing (
                    pub_year,
                    affiliation_id,
                    university,
                    country,
                    total_pub
                )
                WITH country_count AS (
                    SELECT 
                        YEAR(ds.tgl_terbit_scopus) AS pub_year,
                        ps.affiliation_id,
                        MAX(ps.nama_pt) AS university,
                        ucl.country,
                        COUNT(DISTINCT ds.eid_scopus) AS total_pub
                    FROM db_scopus.t_document_scopus ds
                    JOIN db_scopus.t_document_scopus_author dsa 
                        ON dsa.eid_scopus = ds.eid_scopus
                    JOIN db_scopus.t_pt_scopus ps 
                        ON ps.affiliation_id = ds.affiliation_id
                    JOIN db_scopus.t_univ_colaborator_location ucl
                        ON ucl.affiliation_author_id = dsa.affiliation_author_id
                    WHERE 
                        ds.tgl_terbit_scopus >= '2014-01-01' 
                        AND ucl.preferredName <> "" 
                        AND ucl.country <> "" 
                        AND ucl.country <> "Indonesia"
                    GROUP BY 
                        YEAR(ds.tgl_terbit_scopus),
                        ps.affiliation_id,
                        ucl.country
                ),
                ranked AS (
                    SELECT
                        *,
                        ROW_NUMBER() OVER (
                            PARTITION BY pub_year, university
                            ORDER BY total_pub DESC
                        ) AS rn
                    FROM country_count
                )
                SELECT 
                    pub_year,
                    affiliation_id,
                    university,
                    country,
                    total_pub
                FROM ranked
                WHERE rn <= 10
            ON DUPLICATE KEY UPDATE
            university = VALUES(university),
            total_pub = VALUES(total_pub);
        """,
    )

    end_task = EmptyOperator(
        task_id='end_task'
    )

    start_task >> t_top10_international_countries >> end_task
