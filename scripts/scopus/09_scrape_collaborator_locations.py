"""Scraper lokasi afiliasi untuk domain Scopus."""

import time
import logging
import mysql.connector
import undetected_chromedriver as uc

from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import WebDriverException

# =========================
# CONFIG
# =========================
DB_CONFIG = {
    "host": "bira-dwh.binus.db",
    "user": "fitran",
    "password": "F1tran@12345",
    "database": "db_scopus",
}

SCOPUS_LOGIN_URL = "https://www.scopus.com/signin.uri?origin=&zone=TopNavBar"
SCOPUS_HOME_URL = "https://www.scopus.com/"
WAIT_SECONDS = 900  # 15 menit

# =========================
# LOGGING
# =========================
logging.basicConfig(
    filename="scraper.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)

# =========================
# SELENIUM DRIVER FACTORY
# =========================
def create_driver(headless=True):
    options = uc.ChromeOptions()
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-software-rasterizer")

    if headless:
        options.add_argument("--headless=new")

    options.add_argument(
        "--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/115.0.0.0 Safari/537.36"
    )

    return uc.Chrome(options=options, version_main=143)

# =========================
# LOGIN
# =========================
def login_scopus(driver, email, password):
    driver.get(SCOPUS_LOGIN_URL)
    time.sleep(1)
    driver.get(SCOPUS_LOGIN_URL)

    email_txt = '//*[@id="bdd-email"]'
    password_txt = '//*[@id="bdd-password"]'
    cookies_btn = '//*[@id="onetrust-accept-btn-handler"]'
    next_btn = '//*[@id="bdd-elsPrimaryBtn"]'
    login_btn = '//*[@id="bdd-elsPrimaryBtn"]'

    print("==== INI SETELAH PROSES INISIASISASI, SEBELUM LOGIN ===")
    try:
        WebDriverWait(driver, 5).until(
            EC.element_to_be_clickable((By.XPATH, cookies_btn))
        ).click()
    except Exception:
        pass

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, email_txt))
    ).send_keys(email)

    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, next_btn))
    ).click()

    WebDriverWait(driver, 10).until(
        EC.presence_of_element_located((By.XPATH, password_txt))
    ).send_keys(password)

    WebDriverWait(driver, 10).until(
        EC.element_to_be_clickable((By.XPATH, login_btn))
    ).click()

    logging.info("Login Scopus berhasil")

# =========================
# DB UTIL
# =========================
def get_db_connection():
    return mysql.connector.connect(**DB_CONFIG)

# =========================
# DATA FETCH
# =========================
def get_affiliation_id_list():
    query = """
    SELECT DISTINCT affiliation_author_id
    FROM (
        SELECT tdsa.affiliation_author_id 
        FROM t_document_scopus_author tdsa 
        WHERE tdsa.affiliation_author_id NOT IN (
            SELECT tucl.affiliation_author_id 
            FROM t_univ_colaborator_location tucl
        )
    ) t
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(query)
    rows = cursor.fetchall()

    cursor.close()
    conn.close()

    return [r[0] for r in rows if r and r[0]]

# =========================
# SCOPUS API ACCESS
# =========================
def fetch_scopus_affiliation(driver, affiliation_id):
    url = (
        "https://www.scopus.com/gateway/organisation-profile-api/"
        f"organizations/{affiliation_id}"
    )

    driver.get(SCOPUS_HOME_URL)
    time.sleep(1)

    driver.get(url)
    time.sleep(1)

    raw = driver.execute_script("return document.body.innerText;")
    if not raw or not raw.strip().startswith("{"):
        return None

    return driver.execute_script("return JSON.parse(document.body.innerText);")

# =========================
# PROCESS & SAVE
# =========================
def process_and_save_affiliation(driver, affiliation_id):
    data = fetch_scopus_affiliation(driver, affiliation_id)
    if not data:
        logging.warning(f"Data kosong: {affiliation_id}")
        return

    preferred_name = str(data.get("preferredName", "")).strip()
    address = data.get("address") or {}
    country = address.get("country", "")
    city = address.get("city", "")

    insert_sql = """
        INSERT INTO t_univ_colaborator_location
        (affiliation_author_id, preferredName, country, city)
        VALUES (%s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
            preferredName = VALUES(preferredName),
            country = VALUES(country),
            city = VALUES(city)
    """

    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(insert_sql, (affiliation_id, preferred_name, country, city))
    conn.commit()

    cursor.close()
    conn.close()

    logging.info(f"Saved affiliation_id {affiliation_id}")

# =========================
# AIRFLOW ENTRYPOINT
# =========================
def run_scopus_scraper(email, password):
    driver = create_driver(headless=True)
    try:
        
        login_scopus(driver, email, password)
        print("==== INI SETELAH LOGIN ===")

        logging.info("Retrieving affiliation_id. Please wait.... ⌛")
        affiliation_ids = get_affiliation_id_list()

        # Cek None atau empty
        if not affiliation_ids:
            logging.info("affiliation_ids is None or empty. No data to process ❗")
            return

        for aff_id in affiliation_ids:
            process_and_save_affiliation(driver, aff_id)
        logging.info(f"All done saved! ✅")

    finally:
        driver.quit()
