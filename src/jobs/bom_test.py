import logging
from selenium import webdriver
from selenium.webdriver.chrome.options import Options

from bom_test.bom_data import fetch_bom_data_with_selenium, process_and_save_bom_data
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

        df = process_and_save_bom_data(bom_dict)
        if df is None or df.empty:
            logger.error("Processed BOM DataFrame is empty.")
            return False

        logger.info("Inserting data into the database...")
        db_client.ensure_database_exists()
        connection = db_client.get_connection()
        if not connection:
            raise Exception("Failed to get database connection")

        table_name = "bom_data_test"
        df.to_sql(table_name, con=connection, if_exists="append", index=False)
        logger.info(f"Data successfully inserted into PostgreSQL table: {table_name}")
        connection.close()

        logger.info("BOM Data successfully fetched, processed, and saved into DB.")
        return True

    except Exception as e:
        logger.error(f"An error occurred in BOM job: {e}")
        return False
    finally:
        if driver:
            driver.quit()
