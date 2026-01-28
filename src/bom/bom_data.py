import datetime
import logging
import pandas as pd

from typing import Any
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.remote.webdriver import WebDriver
from selenium.webdriver.remote.webelement import WebElement
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

from utils.config import BOM_PASSWORD, BOM_USERNAME

logger = logging.getLogger(__name__)

NoegletalPayload = dict[str, Any]


def _date_range_prev_month_to_first_of_current() -> tuple[str, str]:
    """
    Build a date range from the first day of the previous month up to the first day of the current month.

    :return: Tuple (fra, til) as strings in 'DD-MM-YYYY' format.
    """
    today = datetime.date.today()
    first_day_current_month = today.replace(day=1)
    last_day_previous_month = first_day_current_month - datetime.timedelta(days=1)
    first_day_previous_month = last_day_previous_month.replace(day=1)

    fra = first_day_previous_month.strftime("%d-%m-%Y")
    til = first_day_current_month.strftime("%d-%m-%Y")
    return fra, til


def _date_range_last_12_months_to_first_of_current() -> tuple[str, str]:
    """
    Build a date range from the first day 12 months back up to the first day of the current month.

    :return: Tuple (fra, til) as strings in 'DD-MM-YYYY' format.
    """
    today = datetime.date.today()
    til_date = today.replace(day=1)

    year = til_date.year
    month = til_date.month - 12
    while month <= 0:
        month += 12
        year -= 1

    fra_date = datetime.date(year, month, 1)
    return fra_date.strftime("%d-%m-%Y"), til_date.strftime("%d-%m-%Y")


def _clear_and_type(el: WebElement, text: str) -> None:
    """
    Clear an input element and type new text.

    :param el: Selenium WebElement (typically an input).
    :param text: Text to type into the element.
    :return: None.
    """
    el.click()
    el.send_keys(Keys.CONTROL, "a")
    el.send_keys(Keys.DELETE)
    el.send_keys(text)


def _close_open_multiselect_dropdowns(driver: WebDriver, wait: WebDriverWait) -> None:
    """
    Close any open Bootstrap multiselect dropdowns (if present).

    :param driver: Selenium WebDriver instance.
    :param wait: WebDriverWait instance.
    :return: None.
    """
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


def _set_date_range(driver: WebDriver, wait: WebDriverWait, fra: str, til: str) -> None:
    """
    Set the BOM datepicker range (Fra/Til) and trigger validation.

    :param driver: Selenium WebDriver instance.
    :param wait: WebDriverWait instance.
    :param fra: Start date as 'DD-MM-YYYY'.
    :param til: End date as 'DD-MM-YYYY'.
    :return: None.
    """
    _close_open_multiselect_dropdowns(driver=driver, wait=wait)

    fra_input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#datepicker > input:nth-child(1)")))
    til_input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#datepicker > input:nth-child(2)")))

    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", fra_input)
    _clear_and_type(el=fra_input, text=fra)
    _clear_and_type(el=til_input, text=til)

    # Try TAB on both inputs
    try:
        til_input.send_keys(Keys.TAB)
    except Exception:
        pass


def _click_noegletal_and_wait_refresh(driver: WebDriver, wait: WebDriverWait) -> None:
    """
    Click 'Søg' (if present) and then 'Nøgletal', then wait for the table to refresh.

    :param driver: Selenium WebDriver instance.
    :param wait: WebDriverWait instance.
    :return: None.
    """
    soeg_css = "body > div > div.container > form > div:nth-child(3) > div > span > button.btn.btn-primary"

    before_html = ""
    try:
        before_html = driver.find_element(By.CSS_SELECTOR, "#servicemaal-noegletal-table").get_attribute("innerHTML") or ""
    except Exception:
        pass

    # 1) Click Søg if it exists
    soeg_buttons = driver.find_elements(By.CSS_SELECTOR, soeg_css)
    if soeg_buttons:
        soeg_btn = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, soeg_css)))
        driver.execute_script("arguments[0].scrollIntoView({block:'center'});", soeg_btn)
        soeg_btn.click()

    # 2) Click Nøgletal
    noegletal_btn = wait.until(
        EC.element_to_be_clickable((By.CSS_SELECTOR, "#servicemaal-result-toggler > button:nth-child(2)"))
    )
    noegletal_btn.click()

    # 3) Wait for table to refresh
    def _table_changed(d: WebDriver) -> bool:
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


def _extract_noegletal_payload(driver: WebDriver, wait: WebDriverWait, max_rows: int = 6) -> NoegletalPayload:
    """
    Extract values from the 'Nøgletal' table and return as a JSON payload.

    :param driver: Selenium WebDriver instance.
    :param wait: WebDriverWait instance.
    :param max_rows: Maximum number of table rows to extract (default 6).
    :return: Dictionary payload with dates and lists for 'Kategori', 'Sagsbehandling', 'Servicemal Procent'.
    """
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#servicemaal-noegletal-table tbody tr")))

    fra_val = driver.find_element(By.CSS_SELECTOR, "#datepicker > input:nth-child(1)").get_attribute("value").strip()
    til_val = driver.find_element(By.CSS_SELECTOR, "#datepicker > input:nth-child(2)").get_attribute("value").strip()

    rows = driver.find_elements(By.CSS_SELECTOR, "#servicemaal-noegletal-table tbody tr")[:max_rows]

    kategori: list[str] = []
    sagsbehandling: list[str] = []
    servicemaal_procent: list[str] = []

    for r in rows:
        tds = r.find_elements(By.CSS_SELECTOR, "td")
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


def fetch_bom_data_with_selenium(driver: WebDriver) -> dict[str, NoegletalPayload] | None:
    """
    Log into BOM, navigate to 'Statistik og Servicemål', and extract monthly + glidende gennemsnit 'Nøgletal'.

    :param driver: Selenium WebDriver instance.
    :return: Dict with keys {'monthly', 'glidende_gennemsnit'} on success, otherwise None.
    """
    wait = WebDriverWait(driver, 30)
    login_url = "https://sag.bygogmiljoe.dk/"

    try:
        logger.info("Start BOM RPA job (Selenium)")
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

        _clear_and_type(e=username_input, text=BOM_USERNAME)
        _clear_and_type(e=password_input, text=BOM_PASSWORD)

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

        # Step 7: Servicemål checkboxes (Simple Konstruktioner,  Enfamilieshuse,  Industri og lagerbygninger,  Etagebyggeri, Erhverv &  Etagebyggeri, Boliger)
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
            label_text = label.text.strip()
            logger.info(f"Clicking servicemål: {label_text or sel}")
            label.click()

        _close_open_multiselect_dropdowns(driver=driver, wait=wait)

        # Monthly data extraction
        fra_m, til_m = _date_range_prev_month_to_first_of_current()
        logger.info(f"Setting date range (monthly) Fra={fra_m}, Til={til_m}")
        _set_date_range(driver=driver, wait=wait, fra=fra_m, til=til_m)

        logger.info("Clicking Nøgletal (monthly)...")
        _click_noegletal_and_wait_refresh(driver=driver, wait=wait)
        monthly_payload = _extract_noegletal_payload(driver=driver, wait=wait)

        # Glidende Gennemsnit data extraction
        fra_g, til_g = _date_range_last_12_months_to_first_of_current()
        logger.info(f"Setting date range (glidende_gennemsnit) Fra={fra_g}, Til={til_g}")
        _set_date_range(driver=driver, wait=wait, fra=fra_g, til=til_g)

        logger.info("Clicking Nøgletal (glidende_gennemsnit)...")
        _click_noegletal_and_wait_refresh(driver=driver, wait=wait)
        glidende_payload = _extract_noegletal_payload(driver=driver, wait=wait)
        logger.info("BOM data extracted successfully (monthly + glidende_gennemsnit).")
        return {
            "monthly": monthly_payload,
            "glidende_gennemsnit": glidende_payload,
        }

    except Exception as e:
        logger.error(f"Failed to fetch BOM data with Selenium: {e}")
        return None


def process_and_save_bom_data(
    response_json: dict[str, NoegletalPayload] | None,
) -> tuple[pd.DataFrame | None, pd.DataFrame | None]:
    """
    Transform extracted BOM payloads into pandas DataFrames.

    :param response_json: Response dict from fetch_bom_data_with_selenium().
    :return: Tuple (df_monthly, df_glidende). Returns (None, None) on failure/empty input.
    """
    try:
        if not response_json:
            return None, None

        monthly = response_json.get("monthly") or {}
        glidende = response_json.get("glidende_gennemsnit") or {}

        def _payload_to_df(payload: NoegletalPayload) -> pd.DataFrame:
            """
            Convert a single BOM payload into a DataFrame.

            :param payload: Extracted payload from the Nøgletal table.
            :return: DataFrame with normalized columns.
            """
            kategori = payload.get("Kategori", [])
            sagsbehandling = payload.get("Sagsbehandling", [])
            servicemaal_procent = payload.get("Servicemal Procent", [])
            fra_dato = payload.get("Fra Dato", "")
            til_dato = payload.get("Til Dato", "")

            return pd.DataFrame({
                "Fra Dato": fra_dato,
                "Til Dato": til_dato,
                "Kategori": kategori,
                "Sagsbehandlingstid": sagsbehandling,
                "Servicemål i procent": servicemaal_procent,
            })

        df_monthly = _payload_to_df(payload=monthly)
        df_glidende = _payload_to_df(payload=glidende)

        return df_monthly, df_glidende

    except Exception as e:
        logger.error(f"Failed to process and save data: {e}")
        return None, None
