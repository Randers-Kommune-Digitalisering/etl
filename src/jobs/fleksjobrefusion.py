import logging
from sd_fleksjobrefusion.fleksjobrefusion_data import login_to_sd, process_person
from selenium import webdriver
from utils.api_requests import APIClient
from utils.config import CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_URL, CONFIG_LIBRARY_BASE_PATH, SD_FLEKSJOBREFUSION_CONFIG_FILE
import urllib.parse

logger = logging.getLogger(__name__)

options = webdriver.ChromeOptions()
options.add_argument("--incognito")
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
            t = person["t"]
            i = person["i"]
            b = person["b"]
            l = person["l"]
            if not process_person(driver, t, i, b, l):
                error.append((t, i, b, l))

        if error:
            logger.info("The following persons failed:")
            for t, i, b, l in error:
                logger.error(f"- {t} ({i}): {b} - {l}")
        else:
            logger.info("All persons were processed correctly.")
            return True

    except Exception as e:
        logger.error(f"An error occurred in Fleksjob Refusion job: {e}")
        return False
    finally:
        driver.quit()
