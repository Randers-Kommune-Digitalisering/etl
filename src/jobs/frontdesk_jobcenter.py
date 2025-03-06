import pymssql
import pandas as pd
import logging
import io
# from prophet import Prophet
from datetime import datetime
from utils.config import FRONTDESK_DB_USER, FRONTDESK_DB_PASS, FRONTDESK_DB_HOST, FRONTDESK_DB_DATABASE
from utils.database_connection import get_db_frontdesk

from custom_data_connector import post_data_to_custom_data_connector


logger = logging.getLogger(__name__)

db_client = get_db_frontdesk()


def job():
    logger.info("Initializing frontdesk jobcenter job")
    
    try:
        # workdata = connectToFrontdeskDB()
        workdata = pd.read_csv('data/FrontdeskBorgerserviceTables.csv', sep=';')
    except Exception as e:
        logger.error(e)
        logger.error("Failed to connect to Frontdesk Borgerservice database")
        return False
    else:
        logger.info("Connected to Frontdesk Borgerservice database successfully")