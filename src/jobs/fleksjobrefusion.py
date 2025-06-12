import logging
from sd_fleksjobrefusion.fleksjobrefusion_data import (
    login_to_sd,
    process_person,
    read_excel_from_sftp,
    get_latest_excel_path,
    excel_to_sd_fleksjobrefusion_config,
)
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from utils.sftp_connection import get_sd_sftp_client
logger = logging.getLogger(__name__)

options = Options()
options.add_argument("--incognito")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
driver = webdriver.Chrome(options=options)


def job():
    try:
        logger.info("Starting Fleksjob Refusion job...")

        sftp_client = get_sd_sftp_client()
        REMOTE_EXCEL_PATH = get_latest_excel_path(sftp_client)
        df = read_excel_from_sftp(sftp_client, REMOTE_EXCEL_PATH)
        sd_fleksjobrefusion_config = excel_to_sd_fleksjobrefusion_config(df)

        if not login_to_sd(driver):
            logger.error("Login to SD failed. Exiting job.")
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
