# Scopus Scraping Pipeline

File aktif memakai pola `NN_aksi_objek.py`. Jalankan tahap bernomor kecil lebih
dulu. Jangan menjalankan beberapa tahap yang menulis tabel sama secara bersamaan.

## Urutan

| Tahap | Script | DAG Airflow | Cara jalan |
| --- | --- | --- | --- |
| 01 | `01_scrape_university_affiliations.py` | `scopus_01_scrape_university_affiliations` | Terjadwal tanggal 1 pukul 01:00 WIB |
| 02 | `02_update_university_affiliations.py` | `scopus_02_update_university_affiliations` | Terjadwal tanggal 1 pukul 02:00 WIB |
| 03 | `03_scrape_collaboration_affiliations.py` | `scopus_03_04_collaboration_affiliations` | Task pertama dalam DAG tahap 03-04 |
| 04 | `04_update_collaboration_countries.py` | `scopus_03_04_collaboration_affiliations` | Otomatis setelah tahap 03 |
| 05 | `05_scrape_document_sources.py` | `scopus_05_scrape_document_sources` | Otomatis dipicu setelah tahap 04 |
| 06 | `06_sync_top_20_university_documents.py` | `scopus_06_sync_top_20_university_documents` | Otomatis dipicu setelah tahap 05 |
| 07 | `07_sync_remaining_university_documents.py` | `scopus_07_sync_remaining_university_documents` | Dipicu setelah tahap 06 |
| 08 | `08_sync_all_university_document_details.py` | `scopus_08_sync_all_university_document_details` | Dipicu setelah tahap 06 |
| 09 | `09_scrape_collaborator_locations.py` | Belum aktif | Opsional; memerlukan data author |

Tahap 03-04 terjadwal tanggal 1 pukul 03:00 WIB. Tahap 05 dan 06 diteruskan
otomatis melalui `TriggerDagRunOperator`. Setelah tahap 06 selesai, tahap 07 dan
08 dipicu secara paralel, bukan saling menunggu.

```text
01 -> 02 -> 03 -> 04 -> 05 -> 06 -> 07
                              \-> 08

09 (opsional, DAG masih di archive)
```

Jika menjalankan manual dari Airflow, trigger DAG sesuai nomor di tabel. Tahap 03
dan 04 tidak perlu dipicu terpisah karena berada dalam satu DAG.

## Catatan

- Folder `archive/` berisi backup `.orig` dan DAG nonaktif; Airflow tidak
  menjalankannya.
- Script tahap 07 dan 08 saat ini identik. Nama tetap mengikuti tujuan awalnya,
  tetapi implementasi tahap 08 perlu ditinjau sebelum dianggap sebagai proses
  detail dokumen yang berbeda.
- Mengganti `dag_id` membuat Airflow mencatat run berikutnya di ID baru. Riwayat
  ID lama tetap tersimpan di metadata Airflow.
