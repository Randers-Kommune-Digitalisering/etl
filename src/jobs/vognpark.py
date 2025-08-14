import logging
from vognpark.vognpark_data import (
    read_vognpark_excel_from_sftp,
    get_latest_vognpark_excel_path
)
from utils.database_connection import get_vognpark_db
from utils.sftp_connection import get_shared_sftp_client

logger = logging.getLogger(__name__)

db_client = get_vognpark_db()
sftp_client = get_shared_sftp_client()


def job():
    try:
        logger.info("Starting Vognpark job!")

        excel_path = get_latest_vognpark_excel_path(sftp_client)
        if not excel_path:
            logger.error("No Excel file found to process.")
            return False

        df = read_vognpark_excel_from_sftp(sftp_client, excel_path)
        logger.info(f"Excel data loaded. Rows: {len(df)}")

        logger.info("Inserting data into the database...")
        db_client.ensure_database_exists()
        connection = db_client.get_connection()
        if connection:
            logger.info("Database connection established")
            table_name = "vognpark_data"
            df.to_sql(table_name, con=connection, if_exists='replace', index=False)
            logger.info(f"Data successfully inserted into PostgreSQL table: {table_name}")
            connection.close()
        else:
            logger.error("Failed to get database connection")

        logger.info("Vognpark data successfully fetched, processed, and saved into DB")
        return True

    except Exception as e:
        logger.error(f"Error during Vognpark job: {e}")
        return False
