import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from bom.bom_data import fetch_bom_data_with_selenium, process_and_save_bom_data

from utils.database_connection import get_byggesager_db

logger = logging.getLogger(__name__)
db_client = get_byggesager_db()

options = Options()
options.add_argument("--incognito")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")
options.add_argument("--disable-gpu")
options.add_argument("--headless")
options.add_argument("--window-size=1920,1080")
driver = webdriver.Chrome(options=options)


def job():
    try:
        logger.info("Starting BOM ETL job (Selenium)!")

        bom_dict = fetch_bom_data_with_selenium(driver)
        if not bom_dict:
            logger.error("No BOM data returned from Selenium run.")
            return False

        df_monthly, df_glidende = process_and_save_bom_data(bom_dict)
        if df_monthly is None or df_monthly.empty:
            logger.error("Processed monthly BOM DataFrame is empty.")
            return False
        if df_glidende is None or df_glidende.empty:
            logger.error("Processed rolling 12m BOM DataFrame is empty.")
            return False

        logger.info("Inserting data into the database...")
        db_client.ensure_database_exists()
        connection = db_client.get_connection()
        if not connection:
            raise Exception("Failed to get database connection")

        df_monthly.to_sql("bom_data_monthly", con=connection, if_exists="append", index=False)
        df_glidende.to_sql("bom_data_glidende", con=connection, if_exists="append", index=False)

        logger.info("Data successfully inserted into PostgreSQL tables: bom_data_monthly, bom_data_glidende")
        connection.close()
        return True

    except Exception as e:
        logger.error(f"An error occurred in BOM job: {e}")
        return False
    finally:
        if driver:
            driver.quit()
