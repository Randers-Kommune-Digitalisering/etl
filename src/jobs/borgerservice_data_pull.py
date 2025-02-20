import pymssql
import pandas as pd
import logging
import io


from utils.stfp import SFTPClient
from utils.config import FRONTDESK_DB_USER, FRONTDESK_DB_PASS, FRONTDESK_DB_HOST, FRONTDESK_DB_DATABASE, RANDERS_TEST_SFTP_HOST, RANDERS_TEST_SFTP_USER, RANDERS_TEST_SFTP_PASS, FRONTDESK_DIR


logger = logging.getLogger(__name__)


def job():
    try:
        logger.info("Initializing frontdesk borgerservice data pull job")

        if None not in (RANDERS_TEST_SFTP_HOST, RANDERS_TEST_SFTP_USER, RANDERS_TEST_SFTP_PASS):
            workdata = connectToFrontdeskDBtables()

            file = io.BytesIO(workdata.to_csv(index=False, sep=';').encode('utf-8'))
            filename = "FrontdeskBorgerserviceTables.csv"

            sftp_client = SFTPClient(RANDERS_TEST_SFTP_HOST, RANDERS_TEST_SFTP_USER, RANDERS_TEST_SFTP_PASS)

            with sftp_client.get_connection() as conn:
                conn.putfo(file, f"{FRONTDESK_DIR}/{filename}")

            logger.info(f"{filename} written to SFTP server")

        else:
            logger.warning("SFTP credentials not set - skipping data pull job")

        return True

    except Exception as e:
        logger.error(e)
        return False


def connectToFrontdeskDBtables():
    conn = pymssql.connect(FRONTDESK_DB_HOST, FRONTDESK_DB_USER, FRONTDESK_DB_PASS, FRONTDESK_DB_DATABASE)
    cursor = conn.cursor()

    # cursor.execute("SELECT * FROM information_schema.tables")
    cursor.execute("SELECT * FROM Operation")
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    tables = pd.DataFrame(rows, columns=columns)

    return tables
