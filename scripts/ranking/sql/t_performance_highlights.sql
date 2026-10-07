-- Domain: ranking
TRUNCATE TABLE fact_researchview.t_performance_highlights;

INSERT INTO fact_researchview.t_performance_highlights (year, affiliation_id, nama_pt, total_publications, total_citasi)
WITH summary (
    year,
    affiliation_id,
    nama_pt,
    total_publications,
    total_citasi
) AS (
    SELECT
        LEFT(tds.tgl_terbit_scopus, 4),
        tds.affiliation_id,
        MAX(tps.nama_pt),
        COUNT(tds.eid_scopus),
        SUM(tds.cited_by_scopus)
    FROM db_scopus.t_document_scopus tds
    JOIN db_scopus.t_pt_scopus tps
        ON tps.affiliation_id = tds.affiliation_id
    WHERE tds.tgl_terbit_scopus >= '2014-01-01'
    GROUP BY 1, 2
)
SELECT
    year,
    affiliation_id,
    nama_pt,
    total_publications,
    total_citasi
FROM summary
