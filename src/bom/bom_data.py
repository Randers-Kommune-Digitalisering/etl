from utils.config import BOM_USERNAME, BOM_PASSWORD

import pandas as pd
import logging
import datetime

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)


def _add_months_first_day(d: datetime.date, months: int) -> datetime.date:
    y = d.year
    m = d.month - 1 + months
    y += m // 12
    m = m % 12 + 1
    return datetime.date(y, m, 1)


def _iter_til_months(start_year: int, start_month: int, end_til: datetime.date):
    start = datetime.date(start_year, start_month, 1)
    til = _add_months_first_day(start, 1)
    while til <= end_til:
        yield til
        til = _add_months_first_day(til, 1)


def _iter_month_starts_inclusive(start: datetime.date, end: datetime.date):
    if start.day != 1 or end.day != 1:
        raise ValueError("start og end skal være den 1. i en måned (day=1)")
    cur = start
    while cur <= end:
        yield cur
        cur = _add_months_first_day(cur, 1)


def _clear_and_type(el, text: str):
    el.click()
    el.send_keys(Keys.CONTROL, "a")
    el.send_keys(Keys.DELETE)
    el.send_keys(text)


def _close_open_multiselect_dropdowns(driver, wait: WebDriverWait):
    try:
        if driver.find_elements(By.CSS_SELECTOR, "div.btn-group.open"):
            try:
                driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
            except Exception:
                pass
            try:
                driver.execute_script("document.body.click();")
            except Exception:
                pass

            wait.until(lambda d: len(d.find_elements(By.CSS_SELECTOR, "div.btn-group.open")) == 0)
    except Exception:
        pass


def _set_date_range(driver, wait: WebDriverWait, fra: str, til: str):
    _close_open_multiselect_dropdowns(driver, wait)

    fra_input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#datepicker > input:nth-child(1)")))
    til_input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#datepicker > input:nth-child(2)")))

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", fra_input)
    _clear_and_type(fra_input, fra)
    _clear_and_type(til_input, til)

    # Trigger evt. blur/validering
    try:
        til_input.send_keys(Keys.TAB)
    except Exception:
        pass


def _click_noegletal_and_wait_refresh(driver, wait: WebDriverWait):
    soeg_css = "body > div > div.container > form > div:nth-child(3) > div > span > button.btn.btn-primary"

    before_html = ""
    try:
        before_html = (
            driver.find_element(By.CSS_SELECTOR, "#servicemaal-noegletal-table").get_attribute("innerHTML") or ""
        )
    except Exception:
        pass

    # 1) Klik Søg hvis den findes
    soeg_buttons = driver.find_elements(By.CSS_SELECTOR, soeg_css)
    if soeg_buttons:
        soeg_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, soeg_css)))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", soeg_btn)
        soeg_btn.click()

    # 2) Klik Nøgletal
    noegletal_btn = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "#servicemaal-result-toggler > button:nth-child(2)"))
    )
    noegletal_btn.click()

    # 3) Vent på refresh
    def _table_changed(d):
        try:
            now = d.find_element(By.CSS_SELECTOR, "#servicemaal-noegletal-table").get_attribute("innerHTML") or ""
            if not before_html:
                return len(d.find_elements(By.CSS_SELECTOR, "#servicemaal-noegletal-table tbody tr")) > 0
            return now != before_html and len(now) > 0
        except Exception:
            return False

    try:
        wait.until(_table_changed)
    except Exception:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#servicemaal-noegletal-table tbody tr")))


def _extract_noegletal_payload(driver, wait: WebDriverWait, max_rows: int = 6):

    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#servicemaal-noegletal-table tbody tr")))

    fra_val = driver.find_element(By.CSS_SELECTOR, "#datepicker > input:nth-child(1)").get_attribute("value").strip()
    til_val = driver.find_element(By.CSS_SELECTOR, "#datepicker > input:nth-child(2)").get_attribute("value").strip()

    rows = driver.find_elements(By.CSS_SELECTOR, "#servicemaal-noegletal-table tbody tr")[:max_rows]

    kategori = []
    sagsbehandling = []
    servicemaal_procent = []

    for r in rows:
        tds = r.find_elements(By.CSS_SELECTOR, "td")
        # Kolonneindeks: 1 => td[0], 4 => td[3], 14 => td[13]
        kategori.append(tds[0].text.strip() if len(tds) > 0 else "")
        sagsbehandling.append(tds[3].text.strip() if len(tds) > 3 else "")
        servicemaal_procent.append(tds[13].text.strip() if len(tds) > 13 else "")

    return {
        "Til Dato": til_val,
        "Fra Dato": fra_val,
        "Kategori": kategori,
        "Sagsbehandling": sagsbehandling,
        "Servicemal Procent": servicemaal_procent,
    }


def fetch_bom_data_with_selenium_historical(driver, start_year: int = 2023, start_month: int = 1):
    if not (1 <= start_month <= 12):
        raise ValueError("start_month skal være mellem 1 og 12")

    wait = WebDriverWait(driver, 30)
    login_url = "https://sag.bygogmiljoe.dk/"

    today = datetime.date.today()
    end_til = today.replace(day=1)  # 1. i indeværende måned

    monthly_payloads = []
    glidende_payloads = []

    try:
        logger.info("Start BOM RPA job (Selenium) - historical")
        driver.get(login_url)

        # Step 1: Select kommune
        logger.info("Selecting kommune...")
        kommune_select = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "form div div div select")))
        try:
            Select(kommune_select).select_by_visible_text("Randers Kommune (RPA)")
        except Exception:
            kommune_select.click()
            kommune_select.send_keys("Randers Kommune (RPA)")
            kommune_select.send_keys(Keys.ENTER)

        # Step 2: Click Fortsæt
        logger.info("Clicking Fortsæt...")
        fortsat_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "form div div a")))
        fortsat_btn.click()

        # Step 3: Username + password
        logger.info("Entering username/password...")

        username_input = wait.until(EC.element_to_be_clickable((By.ID, "userNameInput")))
        password_input = wait.until(EC.element_to_be_clickable((By.ID, "passwordInput")))

        _clear_and_type(username_input, BOM_USERNAME)
        _clear_and_type(password_input, BOM_PASSWORD)

        logger.info("Username/password entered.")

        # Step 4: Click Submit button
        logger.info("Clicking Submit...")
        try:
            submit_btn = wait.until(EC.element_to_be_clickable((By.ID, "submitButton")))
            submit_btn.click()

            logger.info("Waiting for BOM to load after login submit...")
            WebDriverWait(driver, 60).until(
                EC.element_to_be_clickable((By.XPATH, "/html/body/div/header/div/div/div[2]/nav"))
            )
            logger.info("Login submit done; BOM loaded.")
        except Exception as e:
            logger.error(f"Failed to click Submit button / wait for BOM to load: {e}")
            raise

        # Step 5: Navigate to "Statistik og Servicemål"
        logger.info("Navigating to Statistik og Servicemål...")
        nav = wait.until(EC.element_to_be_clickable((By.XPATH, "/html/body/div/header/div/div/div[2]/nav")))
        nav.click()

        statistik_link = wait.until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR,
                "body > div > header > div > div > div.span4.offset2 > nav > ul > li > ul > li:nth-child(4) > a"
            ))
        )
        statistik_link.click()

        # Step 6: Sagsområde -> Byg
        logger.info("Setting Sagsområde = Byg...")
        sagomraade_btn = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "form > div:nth-child(2) > div > div:nth-child(2) > button"))
        )
        sagomraade_btn.click()

        byg_checkbox = wait.until(
            EC.element_to_be_clickable((
                By.CSS_SELECTOR,
                "form > div:nth-child(2) > div > div:nth-child(2) > ul > li:nth-child(2) > a > label > input"
            ))
        )
        if not byg_checkbox.is_selected():
            byg_checkbox.click()

        # Step 7: Servicemål checkboxes (5 stk)
        logger.info("Selecting Servicemål checkboxes...")
        servicemaal_btn = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "form > div:nth-child(2) > div > div:nth-child(4) > button"))
        )
        servicemaal_btn.click()

        checkbox_labels = [
            "body > div > div.container > form > div:nth-child(2) > div > div.btn-group.open > ul > li:nth-child(2) > a > label",
            "body > div > div.container > form > div:nth-child(2) > div > div.btn-group.open > ul > li:nth-child(3) > a > label",
            "body > div > div.container > form > div:nth-child(2) > div > div.btn-group.open > ul > li:nth-child(4) > a > label",
            "body > div > div.container > form > div:nth-child(2) > div > div.btn-group.open > ul > li:nth-child(5) > a > label",
            "body > div > div.container > form > div:nth-child(2) > div > div.btn-group.open > ul > li:nth-child(6) > a > label",
        ]

        for sel in checkbox_labels:
            label = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, sel)))
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", label)
            label.click()

        _close_open_multiselect_dropdowns(driver, wait)

        logger.info(
            f"Starting Monthly loop from {start_year}-{start_month:02d} to til={end_til.strftime('%Y-%m-%d')}"
        )
        for til_date in _iter_til_months(start_year, start_month, end_til):
            fra_m = _add_months_first_day(til_date, -1).strftime("%d-%m-%Y")
            til_m = til_date.strftime("%d-%m-%Y")

            _set_date_range(driver, wait, fra_m, til_m)
            _click_noegletal_and_wait_refresh(driver, wait)
            payload_m = _extract_noegletal_payload(driver, wait)
            monthly_payloads.append(payload_m)

        rolling_start_til = datetime.date(start_year, start_month, 1)

        logger.info(
            f"Starting Glidende Gennemsnit loop from til={rolling_start_til.strftime('%Y-%m-%d')} "
            f"to til={end_til.strftime('%Y-%m-%d')}"
        )

        if rolling_start_til <= end_til:
            for til_date in _iter_month_starts_inclusive(rolling_start_til, end_til):
                fra_g = _add_months_first_day(til_date, -12).strftime("%d-%m-%Y")
                til_g = til_date.strftime("%d-%m-%Y")

                _set_date_range(driver, wait, fra_g, til_g)
                _click_noegletal_and_wait_refresh(driver, wait)
                payload_g = _extract_noegletal_payload(driver, wait)
                glidende_payloads.append(payload_g)

        logger.info("BOM historical data extracted successfully.")
        return {
            "monthly": monthly_payloads,
            "glidende_12m": glidende_payloads,
        }

    except Exception as e:
        logger.error(f"Failed to fetch BOM historical data with Selenium: {e}")
        return None


def process_and_save_bom_data_historical(response_json):
    try:
        if not response_json:
            return None, None

        monthly_list = response_json.get("monthly") or []
        glidende_list = response_json.get("glidende_12m") or []

        def _payload_list_to_df(payloads):
            frames = []
            for p in payloads:
                frames.append(pd.DataFrame({
                    "Fra Dato": p.get("Fra Dato", ""),
                    "Til Dato": p.get("Til Dato", ""),
                    "Kategori": p.get("Kategori", []),
                    "Sagsbehandlingstid": p.get("Sagsbehandling", []),
                    "Servicemål i procent": p.get("Servicemal Procent", []),
                }))

            return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()

        df_monthly_hist = _payload_list_to_df(monthly_list)
        df_glidende_hist = _payload_list_to_df(glidende_list)

        return df_monthly_hist, df_glidende_hist

    except Exception as e:
        logger.error(f"Failed to process and save historical BOM data: {e}")
        return None, None
