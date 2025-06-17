import logging
import pandas as pd
from sharepoint.sharepoint_data import get_sharepoint_list
from utils.database_connection import get_sharepoint_db

logger = logging.getLogger(__name__)

db_client = get_sharepoint_db()


def job():
    try:
        logger.info("Starting SharePoint job!")
        sharepoint_data = get_sharepoint_list()
        if not sharepoint_data:
            logger.info("No SharePoint data found.")
            return False
        logger.info(f"Found {len(sharepoint_data)} items in SharePoint List Handleplan – T&D strategi")

        logger.info("Inserting data into the database...")
        db_client.ensure_database_exists()
        connection = db_client.get_connection()
        if connection:
            logger.info("Database connection established")
            table_name = "sharepoint_handleplan_items"
            df = pd.DataFrame(sharepoint_data)
            df.to_sql(table_name, con=connection, if_exists='replace', index=False)
            logger.info(f"Data successfully inserted into PostgreSQL table: {table_name}")
            connection.close()
        else:
            logger.error("Failed to get database connection")

        logger.info("SharePoint data successfully fetched, processed, and saved into DB")
        return True

    except Exception as e:
        logger.error(f"Error during SharePoint job: {e}")
        return False
