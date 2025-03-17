import logging
import pandas as pd
from utils.elastic_search_client import ElasticSearchClient
from utils.config import ELASTICSEARCH_HOST, ELASTICSEARCH_PORT, ELASTICSEARCH_USER, ELASTICSEARCH_PASS
from zylinc.zylinc import query_elasticsearch
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

        scroll_size = 1000
        body = {
            "_source": ["QueueName", "Result", "AgentDisplayName", "ConversationEventType", "TotalDurationInMilliseconds", "EventDurationInMilliseconds", "ConversationEventType"],
            "query": {
                "match": {
                    "QueueName": "IT_Digitalisering_1818"
                }
            },
            "script_fields": {
                "FormattedStartTimeUtc": {
                    "script": {
                        "source": "SimpleDateFormat format = new SimpleDateFormat('yyyy-MM-dd HH:mm:ss'); return format.format(new Date(doc['StartTimeUtc'].value.toInstant().toEpochMilli()));"
                    }
                }
            }
        }

        try:
            data_to_insert = query_elasticsearch(es_client, scroll_size, body)

            try:
                logger.info("Ensuring database exists")
                db_client.ensure_database_exists()
                logger.info("Database exists or created successfully")

                logger.info("Attempting to get database connection")
                with db_client.get_connection() as connection:
                    if connection:
                        logger.info("Database connection established")
                        logger.info("Converting data to DataFrame")
                        df = pd.DataFrame(data_to_insert)
                        logger.info("DataFrame created, inserting data into PostgreSQL database")
                        df.to_sql('zylinc', con=connection, if_exists='replace', index=False, chunksize=1000)
                        logger.info("Data successfully inserted into PostgreSQL database")
                    else:
                        raise Exception("Failed to get database connection")
            except Exception as e:
                logger.error(f"Error inserting data into PostgreSQL: {e}")
                return False

            logger.info("Data saved to PostgreSQL database")
            return True
        except Exception as e:
            logger.error(f"Error querying Elasticsearch: {e}")
            return False
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False
