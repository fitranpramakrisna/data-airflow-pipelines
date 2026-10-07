# =========================================================
# ====================== IMPORT ===========================
# =========================================================

import os
import sys
import time
import json
import random
import re
import mysql.connector
import requests
from datetime import datetime
from typing import Dict
from urllib.parse import urlparse, parse_qs

from dotenv import load_dotenv
from seleniumbase import SB
from bs4 import BeautifulSoup


# =========================================================
# ====================== ENV CONFIG =======================
# =========================================================

load_dotenv()

SCOPUS_USER = os.getenv("SCOPUS_USER")
SCOPUS_PASS = os.getenv("SCOPUS_PASS")

if not SCOPUS_USER or not SCOPUS_PASS:
    raise RuntimeError("SCOPUS_USER / SCOPUS_PASS belum di-set di .env")


# =========================================================
# ====================== DB CONFIG ========================
# =========================================================

mydb = mysql.connector.connect(
    host=os.getenv("DB_HOST"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS"),
    database="db_scopus"
)


# =========================================================
# ====================== LOGIN ============================
# =========================================================

LOGIN_START_URL = (
    "https://id.elsevier.com/as/authorization.oauth2"
    "?platSite=SC%2Fscopus&ui_locales=en-US&scope=openid+profile+email"
    "+els_auth_info+els_analytics_info+urn%3Acom%3Aelsevier%3Aidp%3Apolicy%3Aproduct%3Aindv_identity"
    "&els_policy=idp_policy_indv_identity_plus&response_type=code"
    "&redirect_uri=https%3A%2F%2Fwww.scopus.com%2Fauthredirect.uri"
    "&state=userLogin&authType=SINGLE_SIGN_IN&prompt=login&client_id=SCOPUS"
)


def login_scopus_uc(sb):

    print("🔄 Login ke Scopus (UC Mode)...")

    sb.uc_open_with_reconnect(LOGIN_START_URL, reconnect_time=4)
    sb.sleep(5)

    sb.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    sb.sleep(2)

    sb.execute_script("""
        var btn = document.getElementById('onetrust-accept-btn-handler');
        if (btn) btn.click();
    """)

    sb.sleep(2)

    sb.execute_script("window.scrollTo(0, 0);")
    sb.sleep(1)

    sb.wait_for_element_visible("#bdd-email", timeout=20)
    sb.type("#bdd-email", SCOPUS_USER)

    sb.wait_for_element_visible("#bdd-elsPrimaryBtn", timeout=20)
    sb.uc_click("#bdd-elsPrimaryBtn", reconnect_time=3)

    sb.sleep(3)

    sb.wait_for_element_visible("#bdd-password", timeout=20)
    sb.type("#bdd-password", SCOPUS_PASS)

    sb.uc_click("#bdd-elsPrimaryBtn", reconnect_time=3)

    sb.sleep(4)

    sb.uc_gui_click_captcha()
    sb.sleep(5)

    print("✅ Login selesai")


# =========================================================
# =========== SCRAPE COLLABORATING AFFILIATION ============
# =========================================================

def scrape_collaborating_affiliation(sb):

    cursor = mydb.cursor()

    print("🧹 Mengosongkan tabel collaborating affiliation temp...")

    cursor.execute("""
        TRUNCATE TABLE db_scopus.t_collaborating_affiliation_temp
    """)

    mydb.commit()

    print("📥 Loading affiliation list...")

    cursor.execute("""
        SELECT affiliation_id
        FROM db_scopus.t_pt_scopus
        ORDER BY affiliation_id
    """)

    records = cursor.fetchall()

    total = len(records)

    for no, row in enumerate(records, start=1):

        affiliation = str(row[0])

        print(f"\n[{no}/{total}] AFID : {affiliation}")

        # Buka halaman affiliation terlebih dahulu agar origin & session aktif
        # landing = f"https://www.scopus.com/affil/counts.uri?id={affiliation}"

        # sb.open(landing)
        # sb.sleep(2)

        api_url = (
            f"https://www.scopus.com/gateway/"
            f"organisation-profile-api/organizations/"
            f"{affiliation}/collaborators"
        )

        try:

            result = sb.driver.execute_async_script("""
                const url = arguments[0];
                const callback = arguments[arguments.length - 1];

                fetch(url,{
                    method:'GET',
                    credentials:'include',
                    headers:{
                        'Accept':'application/json'
                    }
                })
                .then(async response => {

                    if(!response.ok){

                        callback({
                            success:false,
                            status:response.status
                        });

                        return;
                    }

                    const data = await response.json();

                    callback({
                        success:true,
                        data:data
                    });

                })
                .catch(err=>{

                    callback({
                        success:false,
                        error:err.toString()
                    });

                });
            """, api_url)

        except Exception as e:

            print("Javascript Error :", e)
            continue

        if not result.get("success"):

            print("Request gagal :", result)
            continue

        collaborators = result["data"].get("collaborators", [])

        print(f"Jumlah collaborator : {len(collaborators)}")

        rows = []

        for item in collaborators:

            rows.append(

                (
                    item.get("id"),
                    item.get("name"),
                    item.get("mutualDocumentsCount"),
                    affiliation
                )

            )

        if rows:

            cursor.executemany("""
                INSERT INTO db_scopus.t_collaborating_affiliation_temp
                (
                    afid_collaboration,
                    nama_affiliation,
                    jml_dokumen,
                    affiliation_id
                )
                VALUES
                (%s,%s,%s,%s)
            """, rows)

            mydb.commit()

            print(f"✔ {len(rows)} data berhasil disimpan")

        else:

            print("Tidak ada collaborator.")

    print("\n======================================")
    print("SCRAPING SELESAI")
    print("======================================")


# =========================================================
# ====================== MAIN =============================
# =========================================================

if __name__ == "__main__":

    with SB(uc=True, headless=False, xvfb=True, incognito=True) as sb:

        login_scopus_uc(sb)

        scrape_collaborating_affiliation(sb)
