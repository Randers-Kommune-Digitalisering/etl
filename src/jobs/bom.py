import logging
import requests
import json
from requests.auth import HTTPBasicAuth
from utils.config import BROWSERLESS_CLIENT_ID, BROWSERLESS_CLIENT_SECRET
from bom.bom_data import get_bom_data, process_and_save_bom_data
from utils.database_connection import get_byggesager_db

logger = logging.getLogger(__name__)

db_client = get_byggesager_db()


def job():
    try:
        logger.info("Starting BOM ETL job!")
        url, headers, data = get_bom_data()

        response = requests.post(url, headers=headers, data=data,
                                 auth=HTTPBasicAuth(BROWSERLESS_CLIENT_ID, BROWSERLESS_CLIENT_SECRET), timeout=180)
        logger.info(f"Response content: {response.content}")

        if response.status_code == 200 and response.content:
            try:
                response_json = response.json()
                df = process_and_save_bom_data(response_json)

                logger.info("Inserting data into the database...")
                db_client.ensure_database_exists()
                connection = db_client.get_connection()
                if connection:
                    logger.info("Database connection established")
                    table_name = "bom_data"
                    df.to_sql(table_name, con=connection, if_exists='replace', index=False)
                    logger.info(f"Data successfully inserted into PostgreSQL table: {table_name}")
                    connection.close()
                else:
                    raise Exception("Failed to get database connection")

                logger.info("BOM Data successfully fetched, processed, and saved into DB.")
                return True
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse response as JSON: {e}")
            except Exception as e:
                logger.error(f"An error occurred while processing data: {e}")
        else:
            logger.error(f"Failed to get a successful response. Status code: {response.status_code}")
    except Exception as e:
        logger.error(f"An error occurred: {e}")
    return False
