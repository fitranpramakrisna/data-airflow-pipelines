import logging
import os
import sys
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
from urllib.parse import quote_plus  # <--- Tambahkan import ini

# Set up logging untuk memantau proses
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)

# Configuration / Database Connection Settings
# Silakan sesuaikan dengan kredensial database Anda
DB_USER = os.getenv("DB_USER", "fitran")
DB_PASSWORD = os.getenv("DB_PASSWORD", "F1tran@12345")
DB_HOST = os.getenv("DB_HOST", "bira-dwh.binus.db")
DB_PORT = os.getenv("DB_PORT", "3306")
DB_NAME = os.getenv("DB_NAME", "fact_researchview")

encoded_user = quote_plus(DB_USER)
encoded_password = quote_plus(DB_PASSWORD)

DATABASE_URL = (
    f"mysql+pymysql://{encoded_user}:{encoded_password}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ----------------------------------------------------------------------
# ETL TASKS DEFINITION
# ----------------------------------------------------------------------
TASKS = [
    {
        "name": "1. t_publication_type_subject_area_stagging",
        "truncate": "TRUNCATE TABLE fact_researchview.t_publication_type_subject_area_stagging;",
        "insert": """
        INSERT INTO fact_researchview.t_publication_type_subject_area_stagging (
            tahun, affiliation_id, nama_pt, type_group, subject_area,
            subjects, total_docs_by_subject_area, total_docs_by_subjects,
            overall_total_docs, overall_total_docs_all_year
        )
        WITH base_docs AS (
            SELECT DISTINCT
                ds.eid_scopus,
                ds.affiliation_id,
                YEAR(ds.tgl_terbit_scopus) AS tahun,
                CASE
                    WHEN ds.type_scopus IN ('Journal', 'Trade Journal', 'Book Series', 'Conference Proceeding', 'Report')
                    THEN ds.type_scopus
                    ELSE 'Unidentified'
                END AS type_group
            FROM db_scopus.t_document_scopus ds
            WHERE ds.tgl_terbit_scopus >= '2014-01-01'
        ),
        eid_faculty AS (
            SELECT DISTINCT
                b.eid_scopus, b.affiliation_id, b.tahun, b.type_group,
                sa.faculty_area, sa.subject_area AS subjects
            FROM base_docs b
            JOIN db_scopus.t_document_scopus_subject_area sa ON b.eid_scopus = sa.eid_scopus
        ),
        faculty_totals AS (
            SELECT tahun, affiliation_id, type_group, faculty_area,
                   COUNT(DISTINCT eid_scopus) AS total_docs_by_faculty_area
            FROM eid_faculty
            GROUP BY tahun, affiliation_id, type_group, faculty_area
        ),
        subject_totals AS (
            SELECT tahun, affiliation_id, type_group, faculty_area, subjects,
                   COUNT(DISTINCT eid_scopus) AS total_docs_by_subjects
            FROM eid_faculty
            GROUP BY tahun, affiliation_id, type_group, faculty_area, subjects
        ),
        overall_totals AS (
            SELECT
                YEAR(tds.tgl_terbit_scopus) AS tahun,
                tds.affiliation_id,
                CASE
                    WHEN tds.type_scopus IN ('Journal', 'Trade Journal', 'Book Series', 'Conference Proceeding', 'Report')
                    THEN tds.type_scopus
                    ELSE 'Unidentified'
                END AS type_group,
                COUNT(DISTINCT tds.eid_scopus) AS overall_total_docs
            FROM db_scopus.t_document_scopus tds
            WHERE tds.tgl_terbit_scopus >= '2014-01-01'
            GROUP BY YEAR(tds.tgl_terbit_scopus), tds.affiliation_id, type_group
        ),
        overall_total_all_year AS (
            SELECT tds.affiliation_id, COUNT(DISTINCT tds.eid_scopus) AS overall_total_docs_all_year
            FROM db_scopus.t_document_scopus tds
            GROUP BY tds.affiliation_id
        )
        SELECT
            ef.tahun, ef.affiliation_id, MAX(tps.nama_pt) AS nama_pt, ef.type_group,
            ef.faculty_area AS subject_area, ef.subjects,
            MAX(ft.total_docs_by_faculty_area) AS total_docs_by_faculty_area,
            MAX(st.total_docs_by_subjects) AS total_docs_by_subjects,
            MAX(ot.overall_total_docs) AS overall_total_docs,
            MAX(ota.overall_total_docs_all_year) AS overall_total_docs_all_year
        FROM eid_faculty ef
        JOIN db_scopus.t_pt_scopus tps ON tps.affiliation_id = ef.affiliation_id
        JOIN faculty_totals ft ON ft.tahun = ef.tahun AND ft.affiliation_id = ef.affiliation_id AND ft.type_group = ef.type_group AND ft.faculty_area = ef.faculty_area
        JOIN subject_totals st ON st.tahun = ef.tahun AND st.affiliation_id = ef.affiliation_id AND st.type_group = ef.type_group AND st.faculty_area = ef.faculty_area AND st.subjects = ef.subjects
        JOIN overall_totals ot ON ot.tahun = ef.tahun AND ot.affiliation_id = ef.affiliation_id AND ot.type_group = ef.type_group
        JOIN overall_total_all_year ota ON ota.affiliation_id = ef.affiliation_id
        GROUP BY ef.tahun, ef.affiliation_id, ef.type_group, ef.faculty_area, ef.subjects;
        """,
    },
    {
        "name": "2. t_journal_by_subject_area_stagging",
        "truncate": "TRUNCATE TABLE fact_researchview.t_journal_by_subject_area_stagging;",
        "insert": """
        INSERT INTO fact_researchview.t_journal_by_subject_area_stagging (
            year, affiliation_id, nama_pt, subject_area, subjects,
            total_docs_by_subject_area, total_docs_subjects
        )
        WITH base_docs AS (
            SELECT DISTINCT ds.eid_scopus, ds.affiliation_id, YEAR(ds.tgl_terbit_scopus) AS tahun
            FROM db_scopus.t_document_scopus ds
            WHERE ds.tgl_terbit_scopus >= '2014-01-01'
        ),
        eid_faculty AS (
            SELECT DISTINCT b.eid_scopus, b.affiliation_id, b.tahun, sa.faculty_area, sa.subject_area AS subjects
            FROM base_docs b
            JOIN db_scopus.t_document_scopus_subject_area sa ON b.eid_scopus = sa.eid_scopus
        ),
        faculty_totals AS (
            SELECT tahun, affiliation_id, faculty_area, COUNT(DISTINCT eid_scopus) AS total_publication
            FROM eid_faculty
            GROUP BY tahun, affiliation_id, faculty_area
        ),
        subject_totals AS (
            SELECT tahun, affiliation_id, faculty_area, subjects, COUNT(DISTINCT eid_scopus) AS total_docs_subjects
            FROM eid_faculty
            GROUP BY tahun, affiliation_id, faculty_area, subjects
        )
        SELECT
            ef.tahun, ef.affiliation_id, MAX(tps.nama_pt) AS nama_pt,
            ef.faculty_area AS subject_area, ef.subjects,
            MAX(ft.total_publication) AS total_docs_by_subject_area,
            MAX(st.total_docs_subjects) AS total_docs_subjects
        FROM eid_faculty ef
        JOIN db_scopus.t_pt_scopus tps ON tps.affiliation_id = ef.affiliation_id
        JOIN faculty_totals ft ON ft.tahun = ef.tahun AND ft.affiliation_id = ef.affiliation_id AND ft.faculty_area = ef.faculty_area
        JOIN subject_totals st ON st.tahun = ef.tahun AND st.affiliation_id = ef.affiliation_id AND st.faculty_area = ef.faculty_area AND st.subjects = ef.subjects
        GROUP BY ef.tahun, ef.affiliation_id, ef.faculty_area, ef.subjects;
        """,
    },
    {
        "name": "3. t_sdg_mapping_subject_area_stagging",
        "truncate": "TRUNCATE TABLE fact_researchview.t_sdg_mapping_subject_area_stagging;",
        "insert": """
        INSERT INTO fact_researchview.t_sdg_mapping_subject_area_stagging (
            tahun, affiliation_id, nama_pt, sdg_label, subject_area,
            subjects, total_docs_by_subject_area, total_docs_by_subjects, overall_total_docs
        )
        WITH base_docs AS (
            SELECT DISTINCT
                ds.eid_scopus, ds.affiliation_id, LEFT(ds.tgl_terbit_scopus, 4) AS tahun,
                CASE
                    WHEN ds.sdg_mapping_scopus IS NULL OR ds.sdg_mapping_scopus = '' OR ds.sdg_mapping_scopus = '0'
                    THEN 'Non-SDG Mapping' ELSE 'SDG Mapping'
                END AS sdg_label
            FROM db_scopus.t_document_scopus ds
            WHERE ds.tgl_terbit_scopus >= '2014-01-01'
        ),
        eid_with_subject AS (
            SELECT DISTINCT b.eid_scopus, b.affiliation_id, b.tahun, b.sdg_label, sa.faculty_area, sa.subject_area AS subjects
            FROM base_docs b
            JOIN db_scopus.t_document_scopus_subject_area sa ON b.eid_scopus = sa.eid_scopus
        ),
        faculty_totals AS (
            SELECT tahun, affiliation_id, sdg_label, faculty_area, COUNT(DISTINCT eid_scopus) AS total_docs_by_faculty_area
            FROM eid_with_subject
            GROUP BY tahun, affiliation_id, sdg_label, faculty_area
        ),
        subject_totals AS (
            SELECT tahun, affiliation_id, sdg_label, faculty_area, subjects, COUNT(DISTINCT eid_scopus) AS total_docs_by_subjects
            FROM eid_with_subject
            GROUP BY tahun, affiliation_id, sdg_label, faculty_area, subjects
        ),
        overall_docs AS (
            SELECT
                LEFT(ds.tgl_terbit_scopus, 4) AS tahun, ds.affiliation_id, ucl.preferredName AS nama_pt,
                CASE
                    WHEN ds.sdg_mapping_scopus IS NULL OR ds.sdg_mapping_scopus = '' OR ds.sdg_mapping_scopus = '0'
                    THEN 'Non-SDG Mapping' ELSE 'SDG Mapping'
                END AS sdg_label,
                COUNT(DISTINCT ds.eid_scopus) AS overall_total_docs
            FROM db_scopus.t_document_scopus ds
            JOIN db_scopus.t_univ_colaborator_location ucl ON ds.affiliation_id = ucl.affiliation_author_id
            WHERE ds.tgl_terbit_scopus >= '2014-01-01'
            GROUP BY LEFT(ds.tgl_terbit_scopus, 4), ds.affiliation_id, ucl.preferredName, sdg_label
        )
        SELECT
            es.tahun, es.affiliation_id, ucl.preferredName AS nama_pt, es.sdg_label,
            es.faculty_area AS subject_area, es.subjects,
            MAX(ft.total_docs_by_faculty_area) AS total_docs_by_subject_area,
            MAX(st.total_docs_by_subjects) AS total_docs_by_subjects,
            MAX(od.overall_total_docs) AS overall_total_docs
        FROM eid_with_subject es
        JOIN db_scopus.t_univ_colaborator_location ucl ON es.affiliation_id = ucl.affiliation_author_id
        JOIN faculty_totals ft ON ft.tahun = es.tahun AND ft.affiliation_id = es.affiliation_id AND ft.sdg_label = es.sdg_label AND ft.faculty_area = es.faculty_area
        JOIN subject_totals st ON st.tahun = es.tahun AND st.affiliation_id = es.affiliation_id AND st.sdg_label = es.sdg_label AND st.faculty_area = es.faculty_area AND st.subjects = es.subjects
        JOIN overall_docs od ON od.tahun = es.tahun AND od.affiliation_id = es.affiliation_id AND od.sdg_label = es.sdg_label
        GROUP BY es.tahun, es.affiliation_id, ucl.preferredName, es.sdg_label, es.faculty_area, es.subjects, od.overall_total_docs;
        """,
    },
    {
        "name": "4. t_publication_collaboration_subject_area_stagging",
        "truncate": "TRUNCATE TABLE fact_researchview.t_publication_collaboration_subject_area_stagging;",
        "insert": """
        INSERT INTO fact_researchview.t_publication_collaboration_subject_area_stagging (
            year, affiliation_id, nama_pt, publication_type, subject_area,
            subjects, total_docs_by_subject_area, total_docs_by_subjects, overall_total_docs
        )
        WITH cte_publication AS (
            SELECT
                LEFT(tds.tgl_terbit_scopus, 4) AS year,
                tps.affiliation_id AS affiliation_id,
                MAX(tps.nama_pt) AS nama_pt,
                tdsa.eid_scopus AS eid_scopus,
                CASE
                    WHEN SUM(CASE WHEN LOWER(tucl.country) <> 'indonesia' THEN 1 END) >= 1 THEN 'international'
                    WHEN SUM(CASE WHEN LOWER(tucl.country) = 'indonesia' AND tps.affiliation_id <> tucl.affiliation_author_id THEN 1 END) >= 1 THEN 'national'
                    WHEN SUM(CASE WHEN tps.affiliation_id = tucl.affiliation_author_id THEN 1 END) >= 1 THEN 'internal'
                    ELSE 'unknown'
                END AS publication_type
            FROM db_scopus.t_document_scopus tds
            JOIN db_scopus.t_document_scopus_author tdsa ON tdsa.eid_scopus = tds.eid_scopus
            JOIN db_scopus.t_pt_scopus tps ON tps.affiliation_id = tds.affiliation_id
            JOIN db_scopus.t_univ_colaborator_location tucl ON tucl.affiliation_author_id = tdsa.affiliation_author_id
            WHERE tds.tgl_terbit_scopus >= '2014-01-01'
            GROUP BY LEFT(tds.tgl_terbit_scopus, 4), tps.affiliation_id, tdsa.eid_scopus
        ),
        cte_with_subject AS (
            SELECT DISTINCT cp.year, cp.affiliation_id, cp.nama_pt, cp.eid_scopus, cp.publication_type, sa.faculty_area, sa.subject_area AS subjects
            FROM cte_publication cp
            JOIN db_scopus.t_document_scopus_subject_area sa ON sa.eid_scopus = cp.eid_scopus
        ),
        faculty_totals AS (
            SELECT year, affiliation_id, publication_type, faculty_area, COUNT(DISTINCT eid_scopus) AS total_docs
            FROM cte_with_subject
            WHERE publication_type IN ('international', 'national', 'internal')
            GROUP BY year, affiliation_id, publication_type, faculty_area
        ),
        subject_totals AS (
            SELECT year, affiliation_id, publication_type, faculty_area, subjects, COUNT(DISTINCT eid_scopus) AS total_docs_by_subject
            FROM cte_with_subject
            WHERE publication_type IN ('international', 'national', 'internal')
            GROUP BY year, affiliation_id, publication_type, faculty_area, subjects
        ),
        cte_overall AS (
            SELECT year, affiliation_id, publication_type, COUNT(*) AS overall_total_docs
            FROM cte_publication
            WHERE publication_type IN ('international', 'national', 'internal')
            GROUP BY year, affiliation_id, publication_type
        )
        SELECT
            cws.year, cws.affiliation_id, MAX(cws.nama_pt) AS nama_pt, cws.publication_type,
            cws.faculty_area AS subject_area, cws.subjects,
            MAX(ft.total_docs) AS total_docs_by_subject_area,
            MAX(st.total_docs_by_subject) AS total_docs_by_subjects,
            co.overall_total_docs
        FROM cte_with_subject cws
        JOIN faculty_totals ft ON ft.year = cws.year AND ft.affiliation_id = cws.affiliation_id AND ft.publication_type = cws.publication_type AND ft.faculty_area = cws.faculty_area
        JOIN subject_totals st ON st.year = cws.year AND st.affiliation_id = cws.affiliation_id AND st.publication_type = cws.publication_type AND st.faculty_area = cws.faculty_area AND st.subjects = cws.subjects
        JOIN cte_overall co ON co.year = cws.year AND co.affiliation_id = cws.affiliation_id AND co.publication_type = cws.publication_type
        WHERE cws.publication_type IN ('international', 'national', 'internal')
        GROUP BY cws.year, cws.affiliation_id, cws.publication_type, cws.faculty_area, cws.subjects, co.overall_total_docs;
        """,
    },
    {
        "name": "5. t_partner_evolution_temp_stagging",
        "truncate": "TRUNCATE TABLE fact_researchview.t_partner_evolution_temp_stagging;",
        "insert": """
        INSERT INTO fact_researchview.t_partner_evolution_temp_stagging (
            year, affiliation_id, nama_pt, partner_collaboration_type, nama_pt_collaborator,
            subject_area, subjects, total_colaborator_by_subjects, total_colaborator_by_subject_area,
            total_colaborator_by_nama_pt, total_colaborator_by_partner_collaboration_type
        )
        WITH base_docs AS (
            SELECT DISTINCT tds.eid_scopus, tds.affiliation_id, LEFT(tds.tgl_terbit_scopus, 4) AS year
            FROM db_scopus.t_document_scopus tds
            WHERE tds.tgl_terbit_scopus >= '2014-01-01'
        ),
        doc_subject AS (
            SELECT DISTINCT b.eid_scopus, b.affiliation_id, b.year, sa.faculty_area AS subject_area, sa.subject_area AS subjects
            FROM base_docs b
            JOIN db_scopus.t_document_scopus_subject_area sa ON sa.eid_scopus = b.eid_scopus
        ),
        doc_partner AS (
            SELECT DISTINCT
                b.eid_scopus, b.affiliation_id, b.year, tucl.preferredName AS nama_pt_collaborator,
                CASE WHEN LOWER(tucl.country) <> 'indonesia' THEN 'international' ELSE 'national' END AS partner_collaboration_type
            FROM base_docs b
            JOIN db_scopus.t_document_scopus_author tdsa ON tdsa.eid_scopus = b.eid_scopus
            JOIN db_scopus.t_univ_colaborator_location tucl ON tucl.affiliation_author_id = tdsa.affiliation_author_id
            JOIN db_scopus.t_pt_scopus tps ON tps.affiliation_id = b.affiliation_id
            WHERE tucl.preferredName IS NOT NULL AND tucl.preferredName <> ''
              AND tucl.affiliation_author_id <> b.affiliation_id
              AND LOWER(tucl.preferredName) <> LOWER(tps.nama_pt)
        ),
        doc_subject_partner AS (
            SELECT DISTINCT ds.eid_scopus, ds.year, ds.affiliation_id, ds.subject_area, ds.subjects, dp.partner_collaboration_type, dp.nama_pt_collaborator
            FROM doc_subject ds
            JOIN doc_partner dp ON dp.eid_scopus = ds.eid_scopus AND dp.affiliation_id = ds.affiliation_id AND dp.year = ds.year
        ),
        collab_type_partners AS (
            SELECT year, affiliation_id, partner_collaboration_type, COUNT(DISTINCT nama_pt_collaborator) AS total_partner_by_collaboration_type
            FROM doc_partner
            GROUP BY year, affiliation_id, partner_collaboration_type
        ),
        partner_docs AS (
            SELECT year, affiliation_id, partner_collaboration_type, nama_pt_collaborator, COUNT(DISTINCT eid_scopus) AS total_docs_by_nama_pt_collaborator
            FROM doc_partner
            GROUP BY year, affiliation_id, partner_collaboration_type, nama_pt_collaborator
        ),
        partner_rank AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY year, affiliation_id, partner_collaboration_type ORDER BY total_docs_by_nama_pt_collaborator DESC) AS rn
            FROM partner_docs
        ),
        subject_area_partners AS (
            SELECT year, affiliation_id, partner_collaboration_type, subject_area, COUNT(DISTINCT nama_pt_collaborator) AS total_partner_by_subject_area
            FROM doc_subject_partner
            GROUP BY year, affiliation_id, partner_collaboration_type, subject_area
        ),
        subjects_partners AS (
            SELECT year, affiliation_id, partner_collaboration_type, subject_area, subjects, COUNT(DISTINCT nama_pt_collaborator) AS total_colaborator_by_subjects
            FROM doc_subject_partner
            GROUP BY year, affiliation_id, partner_collaboration_type, subject_area, subjects
        ),
        partner_subjects_rows AS (
            SELECT DISTINCT year, affiliation_id, partner_collaboration_type, nama_pt_collaborator, subject_area, subjects
            FROM doc_subject_partner
        )
        SELECT
            ps.year, ps.affiliation_id, MAX(tps.nama_pt) AS nama_pt, ps.partner_collaboration_type,
            ps.nama_pt_collaborator, ps.subject_area, ps.subjects,
            sp.total_colaborator_by_subjects,
            sap.total_partner_by_subject_area AS total_colaborator_by_subject_area,
            pd.total_docs_by_nama_pt_collaborator AS total_colaborator_by_nama_pt,
            ctp.total_partner_by_collaboration_type AS total_colaborator_by_partner_collaboration_type
        FROM partner_subjects_rows ps
        JOIN partner_rank pr ON pr.year = ps.year AND pr.affiliation_id = ps.affiliation_id AND pr.partner_collaboration_type = ps.partner_collaboration_type AND pr.nama_pt_collaborator = ps.nama_pt_collaborator AND pr.rn <= 10
        JOIN subjects_partners sp ON sp.year = ps.year AND sp.affiliation_id = ps.affiliation_id AND sp.partner_collaboration_type = ps.partner_collaboration_type AND sp.subject_area = ps.subject_area AND sp.subjects = ps.subjects
        JOIN subject_area_partners sap ON sap.year = ps.year AND sap.affiliation_id = ps.affiliation_id AND sap.partner_collaboration_type = ps.partner_collaboration_type AND sap.subject_area = ps.subject_area
        JOIN partner_docs pd ON pd.year = ps.year AND pd.affiliation_id = ps.affiliation_id AND pd.partner_collaboration_type = ps.partner_collaboration_type AND pd.nama_pt_collaborator = ps.nama_pt_collaborator
        JOIN collab_type_partners ctp ON ctp.year = ps.year AND ctp.affiliation_id = ps.affiliation_id AND ctp.partner_collaboration_type = ps.partner_collaboration_type
        JOIN db_scopus.t_pt_scopus tps ON tps.affiliation_id = ps.affiliation_id
        GROUP BY ps.year, ps.affiliation_id, ps.partner_collaboration_type, ps.nama_pt_collaborator, ps.subject_area, ps.subjects, sp.total_colaborator_by_subjects, sap.total_partner_by_subject_area, pd.total_docs_by_nama_pt_collaborator, ctp.total_partner_by_collaboration_type;
        """,
    },
    {
        "name": "6. t_publication_quartile_subject_area_stagging",
        "truncate": "TRUNCATE TABLE fact_researchview.t_publication_quartile_subject_area_stagging;",
        "insert": """
        INSERT INTO fact_researchview.t_publication_quartile_subject_area_stagging (
            tahun, affiliation_id, nama_pt, quartile_type, subject_area,
            subjects, total_docs_by_subject_area, total_docs_by_subjects, overall_total_docs
        )
        WITH base_docs AS (
            SELECT DISTINCT
                ds.eid_scopus, ds.affiliation_id, LEFT(ds.tgl_terbit_scopus, 4) AS tahun,
                CASE WHEN (ds.quartile = '-' OR ds.quartile IS NULL) THEN 'Non-Q' ELSE ds.quartile END AS quartile_type
            FROM db_scopus.t_document_scopus ds
            WHERE ds.tgl_terbit_scopus >= '2014-01-01'
        ),
        eid_faculty AS (
            SELECT DISTINCT b.eid_scopus, b.affiliation_id, b.tahun, b.quartile_type, sa.faculty_area, sa.subject_area AS subjects
            FROM base_docs b
            JOIN db_scopus.t_document_scopus_subject_area sa ON b.eid_scopus = sa.eid_scopus
        ),
        faculty_totals AS (
            SELECT tahun, affiliation_id, quartile_type, faculty_area, COUNT(DISTINCT eid_scopus) AS total_docs
            FROM eid_faculty
            GROUP BY tahun, affiliation_id, quartile_type, faculty_area
        ),
        subject_totals AS (
            SELECT tahun, affiliation_id, quartile_type, faculty_area, subjects, COUNT(DISTINCT eid_scopus) AS total_docs_by_subjects
            FROM eid_faculty
            GROUP BY tahun, affiliation_id, quartile_type, faculty_area, subjects
        ),
        overall_docs AS (
            SELECT
                LEFT(tds.tgl_terbit_scopus, 4) AS tahun, tps.affiliation_id, MAX(tps.nama_pt),
                CASE WHEN (tds.quartile = '-' OR tds.quartile IS NULL) THEN 'Non-Q' ELSE tds.quartile END AS quartile_type,
                COUNT(tds.eid_scopus) AS overall_total_docs
            FROM db_scopus.t_pt_scopus tps
            JOIN db_scopus.t_document_scopus tds ON tps.affiliation_id = tds.affiliation_id
            WHERE tds.tgl_terbit_scopus >= '2014-01-01'
            GROUP BY 1, 2, 4
        )
        SELECT
            ef.tahun, ef.affiliation_id, MAX(tps.nama_pt) AS nama_pt, ef.quartile_type,
            ef.faculty_area AS subject_area, ef.subjects,
            MAX(ft.total_docs) AS total_docs_by_subject_area,
            MAX(st.total_docs_by_subjects) AS total_docs_by_subjects,
            od.overall_total_docs
        FROM eid_faculty ef
        JOIN db_scopus.t_pt_scopus tps ON tps.affiliation_id = ef.affiliation_id
        JOIN faculty_totals ft ON ft.tahun = ef.tahun AND ft.affiliation_id = ef.affiliation_id AND ft.quartile_type = ef.quartile_type AND ft.faculty_area = ef.faculty_area
        JOIN subject_totals st ON st.tahun = ef.tahun AND st.affiliation_id = ef.affiliation_id AND st.quartile_type = ef.quartile_type AND st.faculty_area = ef.faculty_area AND st.subjects = ef.subjects
        JOIN overall_docs od ON ef.tahun = od.tahun AND ef.affiliation_id = od.affiliation_id AND ef.quartile_type = od.quartile_type
        GROUP BY ef.tahun, ef.affiliation_id, ef.quartile_type, ef.faculty_area, ef.subjects;
        """,
    },
    {
        "name": "7. t_publication_percentile_subject_area_stagging",
        "truncate": "TRUNCATE TABLE fact_researchview.t_publication_percentile_subject_area_stagging;",
        "insert": """
        INSERT INTO fact_researchview.t_publication_percentile_subject_area_stagging (
            year, affiliation_id, nama_pt, percentile_type, subject_area,
            subjects, total_docs_by_subject_area, total_docs_by_subjects, overall_total_docs
        )
        WITH citescore_agg AS (
            SELECT source_id, rank_year, MAX(CASE WHEN source_percentile >= 90 THEN 1 ELSE 0 END) AS is_top_10
            FROM db_scopus.t_citescore_source
            WHERE rank_year >= 2014
            GROUP BY source_id, rank_year
        ),
        eid_status AS (
            SELECT
                CAST(YEAR(tds.tgl_terbit_scopus) AS CHAR) AS year, tds.affiliation_id, tps.nama_pt, tds.eid_scopus,
                CASE
                    WHEN (ca.source_id IS NULL OR ca.is_top_10 = 0) AND tds.quartile = 'Q1' THEN 'Q1 Non Top 10%'
                    WHEN ca.is_top_10 = 1 THEN 'Top 10%'
                    ELSE 'Non Top 10%'
                END AS percentile_type
            FROM db_scopus.t_document_scopus tds
            LEFT JOIN citescore_agg ca ON tds.source_id = ca.source_id AND YEAR(tds.tgl_terbit_scopus) = ca.rank_year
            INNER JOIN db_scopus.t_pt_scopus tps ON tps.affiliation_id = tds.affiliation_id
            WHERE YEAR(tds.tgl_terbit_scopus) >= 2014
        ),
        eid_with_subject AS (
            SELECT DISTINCT es.year, es.affiliation_id, es.nama_pt, es.eid_scopus, es.percentile_type, sa.faculty_area, sa.subject_area AS subjects
            FROM eid_status es
            JOIN db_scopus.t_document_scopus_subject_area sa ON es.eid_scopus = sa.eid_scopus
        ),
        faculty_totals AS (
            SELECT year, affiliation_id, percentile_type, faculty_area, COUNT(DISTINCT eid_scopus) AS total_docs
            FROM eid_with_subject
            GROUP BY year, affiliation_id, percentile_type, faculty_area
        ),
        subject_totals AS (
            SELECT year, affiliation_id, percentile_type, faculty_area, subjects, COUNT(DISTINCT eid_scopus) AS total_docs_by_subjects
            FROM eid_with_subject
            GROUP BY year, affiliation_id, percentile_type, faculty_area, subjects
        ),
        overall_percentile AS (
            SELECT year, affiliation_id, percentile_type, COUNT(DISTINCT eid_scopus) AS overall_total_docs
            FROM eid_status
            GROUP BY year, affiliation_id, percentile_type
        )
        SELECT
            ews.year, ews.affiliation_id, MAX(ews.nama_pt) AS nama_pt, ews.percentile_type,
            ews.faculty_area AS subject_area, ews.subjects,
            MAX(ft.total_docs) AS total_docs_by_subject_area,
            MAX(st.total_docs_by_subjects) AS total_docs_by_subjects,
            op.overall_total_docs
        FROM eid_with_subject ews
        JOIN faculty_totals ft ON ft.year = ews.year AND ft.affiliation_id = ews.affiliation_id AND ft.percentile_type = ews.percentile_type AND ft.faculty_area = ews.faculty_area
        JOIN subject_totals st ON st.year = ews.year AND st.affiliation_id = ews.affiliation_id AND st.percentile_type = ews.percentile_type AND st.faculty_area = ews.faculty_area AND st.subjects = ews.subjects
        JOIN overall_percentile op ON op.year = ews.year AND op.affiliation_id = ews.affiliation_id AND op.percentile_type = ews.percentile_type
        GROUP BY ews.year, ews.affiliation_id, ews.percentile_type, ews.faculty_area, ews.subjects, op.overall_total_docs;
        """,
    },
    {
        "name": "8. t_top_international_countries_subject_area_stagging",
        "truncate": "TRUNCATE TABLE fact_researchview.t_top_international_countries_subject_area_stagging;",
        "insert": """
        INSERT INTO fact_researchview.t_top_international_countries_subject_area_stagging (
            pub_year, affiliation_id, university, country, subject_area,
            subjects, total_docs_by_subject_area, total_docs_by_subjects, overall_total_docs
        )
        WITH country_docs AS (
            SELECT DISTINCT
                ds.eid_scopus, ds.affiliation_id, YEAR(ds.tgl_terbit_scopus) AS pub_year, ucl.country
            FROM db_scopus.t_document_scopus ds
            JOIN db_scopus.t_document_scopus_author dsa ON dsa.eid_scopus = ds.eid_scopus
            JOIN db_scopus.t_univ_colaborator_location ucl ON ucl.affiliation_author_id = dsa.affiliation_author_id
            WHERE ds.tgl_terbit_scopus >= '2014-01-01'
              AND ucl.country <> "" AND ucl.country <> "Indonesia"
        ),
        country_count AS (
            SELECT pub_year, affiliation_id, country, COUNT(DISTINCT eid_scopus) AS total_pub
            FROM country_docs
            GROUP BY pub_year, affiliation_id, country
        ),
        ranked AS (
            SELECT *, ROW_NUMBER() OVER (PARTITION BY pub_year, affiliation_id ORDER BY total_pub DESC) AS rn
            FROM country_count
        ),
        top10 AS (
            SELECT pub_year, affiliation_id, country
            FROM ranked
            WHERE rn <= 10
        ),
        subject_breakdown AS (
            SELECT DISTINCT
                cd.eid_scopus, cd.affiliation_id, cd.pub_year, cd.country,
                sa.faculty_area AS subject_area, sa.subject_area AS subjects
            FROM country_docs cd
            INNER JOIN top10 t10 ON cd.affiliation_id = t10.affiliation_id AND cd.pub_year = t10.pub_year AND cd.country = t10.country
            INNER JOIN db_scopus.t_document_scopus_subject_area sa ON sa.eid_scopus = cd.eid_scopus
        ),
        faculty_totals AS (
            SELECT pub_year, affiliation_id, country, subject_area, COUNT(DISTINCT eid_scopus) AS total_publikasi
            FROM subject_breakdown
            GROUP BY pub_year, affiliation_id, country, subject_area
        ),
        subject_totals AS (
            SELECT pub_year, affiliation_id, country, subject_area, subjects, COUNT(DISTINCT eid_scopus) AS total_docs_by_subjects
            FROM subject_breakdown
            GROUP BY pub_year, affiliation_id, country, subject_area, subjects
        ),
        country_total AS (
            SELECT pub_year, affiliation_id, country, COUNT(DISTINCT eid_scopus) AS overall_total_docs
            FROM subject_breakdown
            GROUP BY pub_year, affiliation_id, country
        )
        SELECT
            sb.pub_year, sb.affiliation_id, MAX(tps.nama_pt) AS university, sb.country,
            sb.subject_area, sb.subjects,
            MAX(ft.total_publikasi) AS total_docs_by_subject_area,
            MAX(st.total_docs_by_subjects) AS total_docs_by_subjects,
            ct.overall_total_docs
        FROM subject_breakdown sb
        JOIN db_scopus.t_pt_scopus tps ON tps.affiliation_id = sb.affiliation_id
        JOIN faculty_totals ft ON ft.pub_year = sb.pub_year AND ft.affiliation_id = sb.affiliation_id AND ft.country = sb.country AND ft.subject_area = sb.subject_area
        JOIN subject_totals st ON st.pub_year = sb.pub_year AND st.affiliation_id = sb.affiliation_id AND st.country = sb.country AND st.subject_area = sb.subject_area AND st.subjects = sb.subjects
        JOIN country_total ct ON ct.pub_year = sb.pub_year AND ct.affiliation_id = sb.affiliation_id AND ct.country = sb.country
        GROUP BY sb.pub_year, sb.affiliation_id, sb.country, sb.subject_area, sb.subjects;
        """,
    },
]

# ----------------------------------------------------------------------
# PIPELINE EXECUTION FUNCTION
# ----------------------------------------------------------------------
def run_etl_pipeline():
    logging.info("Memulai proses ETL Staging Data Research...")
    
    # Inisialisasi Database Engine
    try:
        engine = create_engine(
            DATABASE_URL, 
            pool_recycle=3600, 
            pool_pre_ping=True
        )
        logging.info("Koneksi database berhasil dikonfigurasi.")
    except Exception as e:
        logging.error(f"Gagal menginisialisasi database engine: {e}")
        sys.exit(1)

    # Eksekusi seluruh task di dalam 1 Transaksi Terisolasi
    try:
        with engine.begin() as connection:
            for task in TASKS:
                task_name = task["name"]
                logging.info(f"==> Menjalankan Task: {task_name}")

                # 1. Truncate Table
                logging.info(f"Executing TRUNCATE for {task_name}...")
                connection.execute(text(task["truncate"]))

                # 2. Insert Data
                logging.info(f"Executing INSERT for {task_name}...")
                result = connection.execute(text(task["insert"]))
                
                logging.info(f"Selesai {task_name}. Rows inserted: {result.rowcount}")

        logging.info("==================================================")
        logging.info("SELURUH PROSES ETL STAGING BERHASIL DIEKSEKUSI!")
        logging.info("==================================================")

    except SQLAlchemyError as err:
        logging.error("==================================================")
        logging.error("TERJADI KESALAHAN! Seluruh perubahan di-rollback.")
        logging.error(f"Detail Error SQL: {err}")
        logging.error("==================================================")
        sys.exit(1)
    except Exception as err:
        logging.error(f"Terjadi kesalahan yang tidak terduga: {err}")
        sys.exit(1)


if __name__ == "__main__":
    run_etl_pipeline()