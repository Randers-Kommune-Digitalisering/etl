import logging
import pandas as pd
from utils.elastic_search_client import ElasticSearchClient
from utils.config import ELASTICSEARCH_HOST, ELASTICSEARCH_PORT, ELASTICSEARCH_USER, ELASTICSEARCH_PASS
from zylinc.zylinc import fetch_data_from_elasticsearch, get_queue_names
from utils.database_connection import get_db_zylinc

logger = logging.getLogger(__name__)

db_client = get_db_zylinc()

es_client = ElasticSearchClient(
    host=ELASTICSEARCH_HOST,
    port=ELASTICSEARCH_PORT,
    scheme="https",
    username=ELASTICSEARCH_USER,
    password=ELASTICSEARCH_PASS
)


def job():
    try:
        logger.info('Starting Zylinc ETL job!')

        if not es_client.test_connection():
            logger.error("Failed to connect to Elasticsearch")
            return False

        queue_names = get_queue_names()

        for queue_name in queue_names:
            try:
                logger.info(f"Processing queue: {queue_name}")

                data_to_insert = fetch_data_from_elasticsearch(queue_name, es_client)
                if not data_to_insert:
                    logger.error(f"No data fetched for queue: {queue_name}")
                    return False

                logger.info(f"Inserting data into database for queue: {queue_name}")
                db_client.ensure_database_exists()
                connection = db_client.get_connection()
                if connection:
                    logger.info("Database connection established")
                    df = pd.DataFrame(data_to_insert)
                    table_name = f"zylinc_{queue_name.lower()}"
                    df.to_sql(table_name, con=connection, if_exists='replace', index=False, chunksize=1000)
                    logger.info(f"Data successfully inserted into PostgreSQL table: {table_name}")
                    connection.close()
                else:
                    raise Exception("Failed to get database connection")

                logger.info(f"Queue {queue_name} processed successfully")
            except Exception as e:
                logger.error(f"Error processing queue {queue_name}: {e}")
                return False

        logger.info("All queues processed successfully")
        return True
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False
