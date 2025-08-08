import logging
from sd_fleksjobrefusion.fleksjobrefusion_data import (
    login_to_sd,
    process_person,
    read_excel_from_sftp,
    get_latest_excel_path,
    excel_to_sd_fleksjobrefusion_config,
)
from selenium import webdriver
from mail import send_mail_with_attachment
from utils.utils import df_to_excel_bytes
import datetime
from selenium.webdriver.chrome.options import Options
from utils.sftp_connection import get_sd_sftp_client
import pandas as pd
# import tempfile
from utils.config import SD_FLEKSJOBREFUSION_TO_MAIL, SD_FLEKSJOBREFUSION_FROM_MAIL
logger = logging.getLogger(__name__)

options = Options()
options.add_argument("--incognito")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--headless")
options.add_argument("--window-size=1920,1080")
# options.add_argument(f"--user-data-dir={tempfile.mkdtemp()}")
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
                error.append({
                    "Tjenestenummer": tjenestenummer,
                    "Institution": institution,
                    "Beløb": beloeb,
                    "Lønart": loenart
                })

        if error:
            logger.info("The following persons failed:")
            for e in error:
                logger.error(f"- {e['Tjenestenummer']} ({e['Institution']}): {e['Beløb']} - {e['Lønart']}")

            df_error = pd.DataFrame(error)
            today = datetime.date.today()
            excel_file = df_to_excel_bytes(df_error)

            send_mail_with_attachment(
                to_mail=SD_FLEKSJOBREFUSION_TO_MAIL,
                from_mail=SD_FLEKSJOBREFUSION_FROM_MAIL,
                title=f'Fleksjob Refusion fejl for {today.strftime("%d.%m.%Y")}',
                body='Liste af personer med fejl er vedhæftet.',
                file_name='FleksjobRefusionFejl.xlsx',
                file_bytes=excel_file
            )

            logger.error("Some persons had errors. Check the email for details")
            return False
        else:
            logger.info("All persons were processed correctly.")
            return True

    except Exception as e:
        logger.error(f"An error occurred in Fleksjob Refusion job: {e}")
        return False
    finally:
        driver.quit()
