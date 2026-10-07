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


API_KEYS = []

# ambil dari API key terbesar dulu
for i in range(7, 0, -1):
    k = os.getenv(f"ELSEVIER_API_KEY{i}")
    if k:
        API_KEYS.append(k)

legacy_key = os.getenv("ELSEVIER_API_KEY")

if legacy_key and legacy_key not in API_KEYS:
    API_KEYS.append(legacy_key)

if not API_KEYS:
    raise RuntimeError("Tidak ada API KEY Elsevier di .env")
# ================= DEBUG =================

print("\n========== API KEY CHECK ==========")

for idx, key in enumerate(API_KEYS):

    print(
        f"KEY-{idx} : {key[:10]}********"
    )

print(
    f"TOTAL API KEY : {len(API_KEYS)}"
)

print("===================================\n")

# =========================================

CURRENT_KEY_INDEX = 0

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

    tried_key = 0

    while tried_key < len(API_KEYS):

        key = get_current_api_key()

        print("\n====================================")
        print(f"🔑 API KEY INDEX : {CURRENT_KEY_INDEX}")
        print(f"🔑 API KEY       : {key[:8]}********")
        print("====================================")

        url = url_builder(key)

        print("\n🌍 Requesting API...")
        print(url)

        for retry in range(5):

            try:

                r = requests.get(
                    url,
                    timeout=600,
                    headers={
                        "Accept": "application/json",
                        "X-ELS-APIKey": key
                    }
                )

                print(
                    f"📡 Status: {r.status_code} | Retry: {retry+1}/5"
                )

                # SUCCESS
                if r.status_code == 200:

                    try:
                        data = r.json()

                        total = (
                            data.get("search-results", {})
                            .get("opensearch:totalResults", "unknown")
                        )

                        print(
                            f"✅ Success | Total Result = {total}"
                        )

                    except:
                        pass

                    return r

                # DEBUG RESPONSE
                try:

                    print("\n========== RESPONSE ==========")
                    print(r.text[:3000])
                    print("==============================\n")

                except Exception:
                    pass

                # AUTH / QUOTA ERROR
                if r.status_code in [401, 403, 429]:

                    print(
                        f"⚠️ API KEY ERROR ({r.status_code})"
                    )

                    try:

                        error_json = r.json()

                        print(
                            json.dumps(
                                error_json,
                                indent=2
                            )
                        )

                    except:
                        pass

                    rotate_api_key()

                    tried_key += 1

                    break

                # SERVER ERROR
                if r.status_code in [500, 502, 503, 504]:

                    wait_time = (retry + 1) * 10

                    print(
                        f"⚠️ Elsevier Server Error ({r.status_code})"
                    )

                    print(
                        f"⏳ Retry dalam {wait_time} detik..."
                    )

                    time.sleep(wait_time)

                    continue

                # OTHER ERROR
                r.raise_for_status()

            except requests.exceptions.Timeout:

                wait_time = (retry + 1) * 10

                print(
                    f"⏰ Timeout - Retry {retry+1}/5"
                )

                time.sleep(wait_time)

            except requests.exceptions.ConnectionError as e:

                wait_time = (retry + 1) * 10

                print(
                    f"🌐 Connection Error: {e}"
                )

                time.sleep(wait_time)

            except requests.exceptions.RequestException as e:

                print(
                    f"❌ Request Error: {e}"
                )

                break

        else:

            print(
                "⚠️ Retry habis untuk API KEY ini"
            )

            rotate_api_key()

            tried_key += 1

    raise RuntimeError(
        "Semua API KEY gagal"
    )


# =========================================================
# ====================== PROCESS DOC ======================
# =========================================================

def process_affiliation(mydb, aff_id, start_year, end_year):

    for year in range(start_year, end_year):

        print("\n====================================")
        print("AFID :", aff_id)
        print("YEAR :", year)
        print("====================================")

        query_encoded = f"AF-ID({aff_id})%20%26%20PUBYEAR%20%3D%20{year}"

        def build_total(api_key):

            return (
                "https://api.elsevier.com/content/search/scopus"
                f"?query={query_encoded}"
                f"&apiKey={api_key}"
                "&httpAccept=application%2Fjson"
                "&count=1"
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

            for x in entries:

                try:

                    id_raw = x.get("dc:identifier", "-")

                    if ":" in id_raw:
                        id_scopus = id_raw.split(":")[1]
                    else:
                        id_scopus = id_raw

                    issn = x.get("prism:issn", "-")
                    eissn = x.get("prism:eissn", "-")
                    eid = x.get("eid", "-")
                    title = x.get("dc:title", "-")
                    source = x.get("prism:publicationName", "-")
                    date = x.get("prism:coverDate", "-")
                    citation = x.get("citedby-count", 0)
                    type_ = x.get("prism:aggregationType", "-")
                    volume = x.get("prism:volume", "-")

                    doi_scopus = x.get("prism:doi", "-")

                    try:
                        link = x["link"][2]["@href"]
                    except:
                        link = "-"

                    author_count_raw = x.get("author-count", {})

                    if isinstance(author_count_raw, dict):
                        author_count = int(author_count_raw.get("$", 0))
                    else:
                        author_count = 0

                    cursor = mydb.cursor()

                    cursor.execute(
                        "SELECT id_scopus FROM t_document_scopus WHERE id_scopus=%s and affiliation_id =%s",
                        (id_scopus, aff_id),
                    )

                    records = cursor.fetchall()

                    if not records:

                        cursor.execute(
                            """
                            INSERT INTO t_document_scopus(
                                id_scopus,
                                issn_scopus,
                                eissn_scopus,
                                eid_scopus,
                                title_scopus,
                                tgl_terbit_scopus,
                                nama_source_scopus,
                                link_scopus,
                                cited_by_scopus,
                                type_scopus,
                                volume_scopus,
                                jml_author_scopus,
                                doi_scopus,
                                flag,
                                affiliation_id
                            )
                            VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                            """,
                            (
                                id_scopus,
                                issn,
                                eissn,
                                eid,
                                title,
                                date,
                                source,
                                link,
                                citation,
                                type_,
                                volume,
                                author_count,
                                doi_scopus,
                                0,
                                aff_id,
                            ),
                        )

                        mydb.commit()

                        print("✅ INSERT:", id_scopus)

                    else:

                        print("⏭ SKIP:", id_scopus)

                except Exception as e:

                    print("❌ DB Error:", e)

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
            """
            SELECT affiliation_id
            FROM t_pt_scopus
            WHERE flag = 2
            ORDER BY jml_dokumen_institusi DESC
            LIMIT 1
            FOR UPDATE
            """
        )

        row = cursor.fetchone()

        if not row:
            mydb.commit()
            print("🏁 Semua competitor selesai")
            break

        aff_id = str(row[0])

        print("\n🚀 CLAIM WORKER →", aff_id)

        cursor.execute(
            "UPDATE t_pt_scopus SET flag=1 WHERE affiliation_id=%s",
            (aff_id,),
        )

        mydb.commit()

        try:

            process_affiliation(mydb, aff_id, start_year, end_year)

            cursor.execute(
                "UPDATE t_pt_scopus SET flag=0 WHERE affiliation_id=%s",
                (aff_id,),
            )

            mydb.commit()

            print("✅ DONE:", aff_id)

        except Exception as e:

            cursor.execute(
                "UPDATE t_pt_scopus SET flag=1 WHERE affiliation_id=%s",
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
    print("🚀 Start Sync Competitor (Smaller docs)")
    print("====================================")

    worker_mode(
        start_year=START_YEAR,
        end_year=END_YEAR
    )