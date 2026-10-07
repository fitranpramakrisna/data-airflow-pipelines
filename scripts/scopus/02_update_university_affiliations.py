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
from urllib import response
import requests
from datetime import datetime
from typing import Dict
from urllib.parse import urlparse, parse_qs

from dotenv import load_dotenv
from seleniumbase import SB


# =========================================================
# ====================== ENV CONFIG =======================
# =========================================================

load_dotenv()

SCOPUS_USER = os.getenv("SCOPUS_USER")
SCOPUS_PASS = os.getenv("SCOPUS_PASS")

API_KEY = os.getenv("ELSEVIER_API_KEY7")
INST_TOKEN = os.getenv("ELSEVIER_INSTTOKEN")

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
# ====================== PROCESS AUTHOR ===================
# =========================================================

def update_author_count():

    cursor = mydb.cursor()
    
    # cursor = mydb.cursor()

    cursor.execute("UPDATE db_scopus.t_pt_scopus_temp SET flag=1")
    mydb.commit()
    print(f"✅ Reset flag=1 untuk {cursor.rowcount} baris")

    query = """
    SELECT affiliation_id 
    FROM db_scopus.t_pt_scopus_temp
    WHERE flag=1
    """
#DUA (UPDATE DATA JUMLAH AUTHOR)
# INITTOKEN 82f06eecf62257a8c454aaba6a43ba36 berjalan aman
    # cursor = mydb.cursor()
    # query="SELECT affiliation_id from db_scopus.t_pt_scopus WHERE flag=1"
    cursor.execute(query)
    records = cursor.fetchall()
    mydb.commit()
    for row in records:
        afid = str(row)[2:10]
        #print(afid)
        #API KEY KO ANDREAS
        #url_author='https://api.elsevier.com/content/search/author?query=AF-ID'+repr(row)+'&apiKey=7f59af901d2d86f78a1fd60c1bf9426a'
        #API KEY GUE
        url_author='https://api.elsevier.com/content/search/author?query=AF-ID('+str(afid)+')&apiKey=8f3f253d46bb557e6281ca16149707bf&insttoken=82f06eecf62257a8c454aaba6a43ba36'
        #print(url_author)
        data_tot = requests.get(url_author).json()   
        print(data_tot)
        if 'opensearch:totalResults' in data_tot['search-results']:
            jml_author = data_tot['search-results']['opensearch:totalResults']
            cursor = mydb.cursor()
            cursor.execute("update db_scopus.t_pt_scopus_temp set jml_author=%s, flag=%s where affiliation_id=%s",(str(jml_author),str(0),str(afid)))
            mydb.commit()
            print(str(jml_author)+"=="+str(afid))
            time.sleep(1)
        else:
            print("Ga ada")
            continue
        time.sleep(1)

# =========================================================
# ====================== MAIN =============================
# =========================================================

if __name__ == "__main__":

    with SB(uc=True, headless=False, xvfb=True, incognito=True) as sb:

        login_scopus_uc(sb)

        update_author_count()