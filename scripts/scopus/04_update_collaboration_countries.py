# =========================================================
# ====================== IMPORT ===========================
# =========================================================

import os
import sys
import time
import json
import random
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

    #sb.uc_gui_click_captcha()
    sb.sleep(5)

    print("✅ Login selesai")


# =========================================================
# ====================== UTIL =============================
# =========================================================

def normalize_country(country):

    if not country:
        return "tidak terdeteksi"

    country = country.strip()

    if not country:
        return "tidak terdeteksi"

    if "," in country:
        parts = [p.strip() for p in country.split(",") if p.strip()]
        if len(parts) >= 2:
            return parts[-1]
        return parts[0]

    return country


# =========================================================
# ====================== PROCESS ==========================
# =========================================================

def update_country_for_collab_affiliations(sb):

    cursor = mydb.cursor()

    cursor.execute("""
        SELECT DISTINCT afid_collaboration
        FROM db_scopus.t_collaborating_affiliation_temp
        WHERE negara_affil IS NULL
        AND afid_collaboration IS NOT NULL
        ORDER BY afid_collaboration
    """)

    rows = cursor.fetchall()

    print("Total affiliation:", len(rows))

    for no, row in enumerate(rows, start=1):

        affiliation = str(row[0]).strip()

        print(f"\n[{no}/{len(rows)}] Processing AFID : {affiliation}")

        # buka halaman scopus agar session/origin aktif
        # sb.open(f"https://www.scopus.com/affil/counts.uri?id={affiliation}")
        # sb.sleep(1)

        api_url = (
            f"https://www.scopus.com/gateway/"
            f"organisation-profile-api/organizations/{affiliation}"
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

            print("Javascript Error:", e)

            cursor.execute("""
                UPDATE db_scopus.t_collaborating_affiliation_temp
                SET negara_affil=%s
                WHERE afid_collaboration=%s
            """, ("tidak terdeteksi", affiliation))

            mydb.commit()

            continue

        if "error" in result:

            print(result["error"])

            cursor.execute("""
                UPDATE db_scopus.t_collaborating_affiliation_temp
                SET negara_affil=%s
                WHERE afid_collaboration=%s
            """, ("tidak terdeteksi", affiliation))

            mydb.commit()

            continue

        # ======================
        # Ambil country
        # ======================

        # country = None

        # address = result.get("address")

        # if isinstance(address, dict):

        #     country = address.get("country")

        # normalized = normalize_country(country)

        data = result.get("data", {})

        country = None

        address = data.get("address")

        if isinstance(address, dict):
            country = address.get("country")

        normalized = normalize_country(country)

        print("Country :", normalized)

        # print("Country :", normalized)

        cursor.execute("""
            UPDATE db_scopus.t_collaborating_affiliation_temp
            SET negara_affil=%s
            WHERE afid_collaboration=%s
        """, (normalized, affiliation))

        mydb.commit()

        time.sleep(0.3)

    print("\n==============================")
    print("UPDATE COUNTRY SELESAI")
    print("==============================")

# =========================================================
# ====================== MAIN =============================
# =========================================================

if __name__ == "__main__":

    with SB(uc=True, headless=False, xvfb=True, incognito=True) as sb:

        login_scopus_uc(sb)

        update_country_for_collab_affiliations(sb)