from utils.config import BOM_USERNAME, BOM_PASSWORD
import pandas as pd
import logging
import datetime

from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait, Select
from selenium.webdriver.support import expected_conditions as EC

logger = logging.getLogger(__name__)


def _date_range_prev_month_to_first_of_current():
    today = datetime.date.today()
    first_day_current_month = today.replace(day=1)
    last_day_previous_month = first_day_current_month - datetime.timedelta(days=1)
    first_day_previous_month = last_day_previous_month.replace(day=1)

    fra = first_day_previous_month.strftime("%d-%m-%Y")
    til = first_day_current_month.strftime("%d-%m-%Y")
    return fra, til


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


def fetch_bom_data_with_selenium(driver):
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

        # Step 7: Servicemål checkboxes
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

        _close_open_multiselect_dropdowns(driver, wait)

        # Step 8: Date range
        fra_default, til_default = _date_range_prev_month_to_first_of_current()
        logger.info(f"Setting date range Fra={fra_default}, Til={til_default}")

        fra_input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#datepicker > input:nth-child(1)")))
        til_input = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "#datepicker > input:nth-child(2)")))

        _clear_and_type(fra_input, fra_default)
        _clear_and_type(til_input, til_default)

        # Step 9: Click "Nøgletal"
        logger.info("Clicking Nøgletal...")
        noegletal_btn = wait.until(
            EC.element_to_be_clickable((By.CSS_SELECTOR, "#servicemaal-result-toggler > button:nth-child(2)"))
        )
        noegletal_btn.click()

        # Step 10: Extract table data
        logger.info("Extracting Nøgletal table...")
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, "#servicemaal-noegletal-table tbody tr")))

        fra_val = driver.find_element(By.CSS_SELECTOR, "#datepicker > input:nth-child(1)").get_attribute("value").strip()
        til_val = driver.find_element(By.CSS_SELECTOR, "#datepicker > input:nth-child(2)").get_attribute("value").strip()

        rows = driver.find_elements(By.CSS_SELECTOR, "#servicemaal-noegletal-table tbody tr")
        rows = rows[:6]

        kategori = []
        sagsbehandling = []
        servicemaal_procent = []

        for r in rows:
            tds = r.find_elements(By.CSS_SELECTOR, "td")
            kategori.append(tds[0].text.strip() if len(tds) > 0 else "")
            sagsbehandling.append(tds[3].text.strip() if len(tds) > 3 else "")
            servicemaal_procent.append(tds[13].text.strip() if len(tds) > 13 else "")

        logger.info("BOM data extracted successfully.")
        return {
            "Til Dato": til_val,
            "Fra Dato": fra_val,
            "Kategori": kategori,
            "Sagsbehandling": sagsbehandling,
            "Servicemal Procent": servicemaal_procent,
        }

    except Exception as e:
        logger.error(f"Failed to fetch BOM data with Selenium: {e}")
        return None


def process_and_save_bom_data(response_json):
    try:
        if not response_json:
            return None

        kategori = response_json.get("Kategori", [])
        sagsbehandling = response_json.get("Sagsbehandling", [])
        servicemaal_procent = response_json.get("Servicemal Procent", [])
        fra_dato = response_json.get("Fra Dato", "")
        til_dato = response_json.get("Til Dato", "")

        logger.info(f"Fra Dato: {fra_dato}, Til Dato: {til_dato}")
        logger.info(f"Kategori: {kategori}")
        logger.info(f"Sagsbehandling: {sagsbehandling}")
        logger.info(f"Servicemål i procent: {servicemaal_procent}")

        df = pd.DataFrame({
            "Fra Dato": fra_dato,
            "Kategori": kategori,
            "Sagsbehandlingstid": sagsbehandling,
            "Servicemål i procent": servicemaal_procent
        })

        return df
    except Exception as e:
        logger.error(f"Failed to process and save data: {e}")
        return None
