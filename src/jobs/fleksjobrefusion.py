import logging
from sd_fleksjobrefusion.fleksjobrefusion_data import login_to_sd, process_person
from selenium import webdriver
from utils.api_requests import APIClient
from selenium.webdriver.chrome.options import Options
from utils.config import CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_URL, CONFIG_LIBRARY_BASE_PATH, SD_FLEKSJOBREFUSION_CONFIG_FILE
import urllib.parse

logger = logging.getLogger(__name__)

options = Options()
options.add_argument("--incognito")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
driver = webdriver.Chrome(options=options)

config_library_client = APIClient(base_url=CONFIG_LIBRARY_URL, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)


def job():
    try:
        logger.info("Starting Fleksjob Refusion job...")
        if not login_to_sd(driver):
            logger.error("Login to SD failed. Exiting job.")
            return False

        config_path = urllib.parse.urljoin(CONFIG_LIBRARY_BASE_PATH, SD_FLEKSJOBREFUSION_CONFIG_FILE)
        sd_fleksjobrefusion_config = config_library_client.make_request(path=config_path)
        if sd_fleksjobrefusion_config is None:
            logging.error(f"Failed to load config file from path: {config_path}")
            return False

        error = []
        for person in sd_fleksjobrefusion_config:
            tjenestenummer = person["tjenestenummer"]
            institution = person["institution"]
            beloeb = person["beloeb"]
            loenart = person["loenart"]
            if not process_person(driver, tjenestenummer, institution, beloeb, loenart):
                error.append((tjenestenummer, institution, beloeb, loenart))

        if error:
            logger.info("The following persons failed:")
            for tjenestenummer, institution, beloeb, loenart in error:
                logger.error(f"- {tjenestenummer} ({institution}): {beloeb} - {loenart}")
            return False
        else:
            logger.info("All persons were processed correctly.")
            return True

    except Exception as e:
        logger.error(f"An error occurred in Fleksjob Refusion job: {e}")
        return False
    finally:
        driver.quit()
