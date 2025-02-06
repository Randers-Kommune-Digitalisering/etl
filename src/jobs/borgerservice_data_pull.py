import pymssql
import pandas as pd
import logging
import io

from utils.config import FRONTDESK_DB_USER, FRONTDESK_DB_PASS, FRONTDESK_DB_HOST, FRONTDESK_DB_DATABASE
from custom_data_connector import post_data_to_custom_data_connector

logging.getLogger("prophet.plot").disabled = True
logger = logging.getLogger(__name__)


def job():
    logger.info("Initializing frontdesk borgerservice data pull job")

    workdata = connectToFrontdeskDBtables()

    file = io.BytesIO(workdata.to_csv(index=False, sep=';').encode('utf-8'))
    filename = "SA" + "FrontdeskBorgerserviceTables" + ".csv"

    if post_data_to_custom_data_connector(filename, file):
        logger.info("Updated Frontdesk Borgerservice data successfully")
        return True
    else:
        logger.error("Failed to update Frontdesk Borgerservice data")
        return False


def connectToFrontdeskDBtables():
    conn = pymssql.connect(FRONTDESK_DB_HOST, FRONTDESK_DB_USER, FRONTDESK_DB_PASS, FRONTDESK_DB_DATABASE)
    cursor = conn.cursor()

    # cursor.execute("SELECT * FROM information_schema.tables")
    cursor.execute("SELECT * FROM Feedback")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    tables = pd.DataFrame(rows, columns=columns)

    return tables
