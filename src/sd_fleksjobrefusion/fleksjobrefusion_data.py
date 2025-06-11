import time
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.keys import Keys
from utils.config import SD_FLEKSJOBREFUSION_USERNAME, SD_FLEKSJOBREFUSION_PASSWORD, SD_FLEKSJOBREFUSION_URL
import logging
logger = logging.getLogger(__name__)


def login_to_sd(driver):
    try:
        logger.info("Navigating to login page...")
        driver.get(SD_FLEKSJOBREFUSION_URL)

        logger.info("Waiting for 'Log In' button...")
        log_in_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="module-116"]/div/div/div/div/a'))
        )
        log_in_button.click()
        logger.info("'Log Ind' button clicked.")

        logger.info("Waiting for 'Arbejdsplads-View' button...")
        workplace_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="arbejdspladsButton"]'))
        )
        workplace_button.click()
        logger.info("'Arbejdsplads-View' button clicked.")

        if len(driver.window_handles) > 1:
            driver.switch_to.window(driver.window_handles[-1])

        logger.info("Switching to iframe 'iframe-oiosaml'...")
        WebDriverWait(driver, 20).until(
            EC.frame_to_be_available_and_switch_to_it((By.ID, "iframe-oiosaml"))
        )

        logger.info("Waiting for Dropdown menu element to be clickable...")
        select_element = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/div/select'))
        )
        select_element.click()
        select_element.send_keys('R')
        logger.info("Dropdown menu Randers Kommune Selected.")

        logger.info("Waiting for Arbejdsplads-log in Button to be clickable...")
        workplace_login_button = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/div/input'))
        )
        workplace_login_button.click()
        logger.info("Arbejdsplads Login button clicked.")

        # Uncomment the following lines if you run this locally inside Randers Kommune Network/ADFS
        # time.sleep(2)
        # pyautogui.write(SD_FLEKSJOBREFUSION_USERNAME)
        # pyautogui.press('tab')
        # pyautogui.write(SD_FLEKSJOBREFUSION_PASSWORD)
        # pyautogui.press('enter')

        time.sleep(2)
        logger.info("Entering username...")
        username_input = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="userNameInput"]'))
        )
        username_input.clear()
        username_input.send_keys(SD_FLEKSJOBREFUSION_USERNAME)
        logger.info("Username entered.")
        time.sleep(1)

        logger.info("Entering password...")
        password_input = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="passwordInput"]'))
        )
        password_input.clear()
        password_input.send_keys(SD_FLEKSJOBREFUSION_PASSWORD)
        logger.info("Password entered.")
        time.sleep(1)

        logger.info("Clicking submit button...")
        submit_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="submitButton"]'))
        )
        submit_button.click()
        logger.info("Submit button clicked.")

        time.sleep(5)
        driver.switch_to.default_content()

        personaleweb_button = WebDriverWait(driver, 30).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="product-cf662da2-9d3c-0108-e043-0a10f6400108"]/div'))
        )
        personaleweb_button.click()

        WebDriverWait(driver, 10).until(lambda d: len(d.window_handles) > 1)
        driver.switch_to.window(driver.window_handles[-1])
        logger.info("Login completed and switched to Personaleweb tab.")

    except Exception as e:
        logger.error(f"Login failed: {e}")
        return False

    return True


def process_person(driver, tjenestenummer, institution, beloeb, loenart):
    try:

        logger.info("Entering SD Personaleweb...")

        # Search for the person
        logger.info("Clicking on the search field...")
        search_field = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/div[3]/div/div/div/div/div[2]/div[2]/input'))
        )
        search_field.clear()
        search_field.send_keys(f'{tjenestenummer} {institution}')
        search_field.click()
        logger.info(f"Searching for person with tjenestenummer: {tjenestenummer} and institution: {institution}")

        logger.info("Clicking on the first name in the search results...")
        name_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '/html/body/ul/li[1]'))
        )
        name_button.click()

        # Switch to iframe 'insideiframe'
        logger.info("Switching to iframe 'insideiframe'...")
        WebDriverWait(driver, 20).until(
            EC.frame_to_be_available_and_switch_to_it((By.ID, "insideiframe"))
        )

        # Switch to frame under 'jbFrameset'
        logger.info("Switching to frame under 'jbFrameset'...")
        WebDriverWait(driver, 20).until(
            EC.frame_to_be_available_and_switch_to_it((By.XPATH, '//*[@id="jbFrameset"]/frame'))
        )

        # Click on Indberetning
        logger.info("Clicking on Indberetning...")
        indberetning_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="fe19"]'))
        )
        indberetning_button.click()

        # Click on Merarbejde
        logger.info("Clicking on Merarbejde...")
        merarbejde_button = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="tab_2711"]'))
        )
        merarbejde_button.click()

        logger.info("Switching to default content before switching frames...")
        driver.switch_to.default_content()

        # Switch to 'insideiframe'
        logger.info("Switching to 'insideiframe'...")
        WebDriverWait(driver, 20).until(
            EC.frame_to_be_available_and_switch_to_it((By.ID, "insideiframe"))
        )

        # Switch to 'insidemain' frame in 'innerFrameset'
        logger.info("Switching to 'insidemain' frame in 'innerFrameset'...")
        WebDriverWait(driver, 20).until(
            EC.frame_to_be_available_and_switch_to_it((By.XPATH, '//frameset[@id="innerFrameset"]/frame[@name="insidemain"]'))
        )

        # Switch to 'merarbejde' frame
        logger.info("Switching to 'merarbejde' frame...")
        WebDriverWait(driver, 20).until(
            EC.frame_to_be_available_and_switch_to_it((By.NAME, "merarbejde"))
        )

        # Wait for the amount input to be available and clickable
        logger.info("Waiting for the amount input field...")
        beloeb_input = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="pageForm:beloeb"]'))
        )
        beloeb_input.click()
        beloeb_input.clear()
        beloeb_input.send_keys(str(beloeb))
        logger.info(f"Amount input set to: {beloeb} kr.")

        # Wait for the wage type input to be available and clickable
        logger.info("Waiting for the wage type input field...")
        loenart_input = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="pageForm:loenart_input"]'))
        )
        loenart_input.click()
        loenart_input.clear()
        loenart_input.send_keys(str(loenart))
        time.sleep(1)
        loenart_input.send_keys(Keys.ENTER)
        logger.info(f"Wage type input set to: {loenart}")

        time.sleep(2)

        # Wait for the approved input to be available and clickable
        logger.info("Waiting for the approved input field...")
        godkendt_input = WebDriverWait(driver, 20).until(
            EC.element_to_be_clickable((By.XPATH, '//*[@id="pageForm:godkendt"]'))
        )
        time.sleep(1)
        godkendt_input.click()
        logger.info("Approved input clicked.")

        time.sleep(2)

        logger.info(f"✅ {tjenestenummer} ({institution}) behandlet med beløb {beloeb} og lønart {loenart}.")
        return True

    except Exception as e:
        logger.error(f"❌ Fejl for {tjenestenummer}: {e}")
        return False

    finally:
        driver.switch_to.default_content()
