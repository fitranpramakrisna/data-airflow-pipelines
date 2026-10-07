# =========================================================
# ====================== IMPORT ===========================
# =========================================================

import os
import time
import json
import mysql.connector
import requests

from dotenv import load_dotenv

# =========================================================
# ====================== ENV CONFIG =======================
# =========================================================

load_dotenv()

# ================= API KEY =================

API_KEYS = []

for i in range(1, 6):
    k = os.getenv(f"ELSEVIER_API_KEY{i}")
    if k:
        API_KEYS.append(k)

legacy_key = os.getenv("ELSEVIER_API_KEY")

if legacy_key and legacy_key not in API_KEYS:
    API_KEYS.insert(0, legacy_key)

if not API_KEYS:
    raise RuntimeError("Tidak ada API KEY Elsevier di .env")

CURRENT_KEY_INDEX = 0


def get_current_api_key():
    return API_KEYS[CURRENT_KEY_INDEX]


def rotate_api_key():
    global CURRENT_KEY_INDEX

    CURRENT_KEY_INDEX += 1

    if CURRENT_KEY_INDEX >= len(API_KEYS):
        raise RuntimeError("Semua API key habis")

    print("⚠️ Switch API KEY →", CURRENT_KEY_INDEX)


# =========================================================
# ====================== DB CONFIG ========================
# =========================================================

DB_NAME = os.getenv("DB_SCOPUS")

if not DB_NAME:
    raise RuntimeError("DB_SCOPUS belum diset di .env")

mydb = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    database=DB_NAME
)

print("✅ Connected to DB:", DB_NAME)

# =========================================================
# ====================== API REQUEST ======================
# =========================================================

def scopus_request(url_builder):

    tried = 0

    while tried < len(API_KEYS):

        key = get_current_api_key()

        url = url_builder(key)

        print("🌍 Requesting API...")

        try:

            r = requests.get(url, timeout=60)

            print("📡 Status:", r.status_code)

            if r.ok:
                return r

            if r.status_code in [401, 403, 429]:

                print("⚠️ API key quota / forbidden")

                rotate_api_key()
                tried += 1
                continue

            r.raise_for_status()

        except Exception as e:

            print("❌ Request error:", e)

            rotate_api_key()

            tried += 1

    raise RuntimeError("Semua API key gagal")


# =========================================================
# ====================== PROCESS DOC ======================
# =========================================================

def process_affiliation(mydb, aff_id, start_year, end_year):

    for year in range(start_year, end_year):

        print("\n====================================")
        print("AFID :", aff_id)
        print("YEAR :", year)
        print("====================================")

        # aff_id='60122670'

        query_encoded = f"AF-ID({aff_id})%20%26%20PUBYEAR%20%3D%20{year}"

        def build_total(api_key):

            return (
                "https://api.elsevier.com/content/search/scopus"
                f"?query={query_encoded}"
                f"&apiKey={api_key}"
                "&httpAccept=application%2Fjson"
            )

        resp = scopus_request(build_total)

        data = resp.json()

        total = int(
            data.get("search-results", {})
            .get("opensearch:totalResults", 0)
        )

        print("📄 TOTAL DOCUMENT:", total)

        if total == 0:
            continue

        start = 0
        count = 25

        page = 1
        total_page = (total // count) + 1

        while start < total:

            print(f"📑 PAGE {page}/{total_page} | start={start}")

            def build_page(api_key):

                return (
                    "https://api.elsevier.com/content/search/scopus"
                    f"?query={query_encoded}"
                    f"&apiKey={api_key}"
                    "&httpAccept=application%2Fjson"
                    f"&start={start}&count={count}"
                )

            resp = scopus_request(build_page)

            data = resp.json()

            entries = data.get("search-results", {}).get("entry", [])

            insert_count = 0
            update_count = 0

            for x in entries:

                try:

                    id_raw = x.get("dc:identifier", "-")

                    if ":" in id_raw:
                        id_scopus = id_raw.split(":")[1]
                    else:
                        id_scopus = id_raw

                    eid = x.get("eid", "-")
                    title = x.get("dc:title", "-")
                    source = x.get("prism:publicationName", "-")
                    date = x.get("prism:coverDate", "-")
                    citation = x.get("citedby-count", 0)
                    type_ = x.get("prism:aggregationType", "-")

                    cursor = mydb.cursor()

                    cursor.execute(
                        """
                        SELECT id_scopus
                        FROM t_document_scopus
                        WHERE id_scopus = %s
                        AND affiliation_id = %s
                        """,
                        (id_scopus, aff_id),
                    )

                    # cursor.execute(
                    #     "SELECT id_scopus FROM t_document_scopus WHERE id_scopus=%s
                    #     AND affiliation_id = %s",
                    #     (id_scopus,),
                    # )

                    records = cursor.fetchall()

                    if not records:

                        cursor.execute(
                            """
                            INSERT INTO t_document_scopus(
                                id_scopus,
                                eid_scopus,
                                title_scopus,
                                tgl_terbit_scopus,
                                nama_source_scopus,
                                cited_by_scopus,
                                type_scopus,
                                flag,
                                affiliation_id
                            )
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """,
                            (
                                id_scopus,
                                eid,
                                title,
                                date,
                                source,
                                citation,
                                type_,
                                0,
                                aff_id,
                            ),
                        )

                        mydb.commit()

                        insert_count += 1

                    else:

                        cursor.execute(
                            """
                            UPDATE t_document_scopus
                            SET cited_by_scopus=%s
                            WHERE eid_scopus=%s
                            """,
                            (citation, eid),
                        )

                        mydb.commit()

                        update_count += 1

                except Exception as e:

                    print("❌ DB Error:", e)

            print(f"✅ Inserted: {insert_count} | Updated: {update_count}")

            start += count
            page += 1

            time.sleep(0.5)


# =========================================================
# ====================== WORKER MODE ======================
# =========================================================

def worker_mode(start_year, end_year):

    while True:

        cursor = mydb.cursor()

        mydb.start_transaction()

        cursor.execute(
            "SELECT affiliation_id FROM t_pt_scopus WHERE flag=1 ORDER BY jml_dokumen_institusi DESC  LIMIT 1 FOR UPDATE"
        )

        row = cursor.fetchone()

        if not row:
            mydb.commit()
            print("🏁 Semua affiliation selesai")
            break

        aff_id = str(row[0])

        print("\n🚀 CLAIM WORKER →", aff_id)

        cursor.execute(
            "UPDATE t_pt_scopus SET flag=2 WHERE affiliation_id=%s",
            (aff_id,),
        )

        mydb.commit()

        try:

            process_affiliation(mydb, aff_id, start_year, end_year)

            cursor.execute(
                "UPDATE t_pt_scopus SET flag=2 WHERE affiliation_id=%s",
                (aff_id,),
            )

            mydb.commit()

            print("✅ DONE:", aff_id)

        except Exception as e:

            cursor.execute(
                "UPDATE t_pt_scopus SET flag=0 WHERE affiliation_id=%s",
                (aff_id,),
            )

            mydb.commit()

            print("❌ ERROR:", e)


# =========================================================
# ====================== MAIN =============================
# =========================================================

if __name__ == "__main__":

    START_YEAR = 2014
    END_YEAR = 2026

    print("\n====================================")
    print("🚀 Start Sync Document Scopus")
    print("====================================")

    worker_mode(
        start_year=START_YEAR,
        end_year=END_YEAR
    )