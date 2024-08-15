import logging
import pymssql

from utils.config import SERVER_INVENTORY_DB_NAME, SERVER_INVENTORY_DB_USER, SERVER_INVENTORY_DB_PASS, SERVER_INVENTORY_DB_HOST

logger = logging.getLogger(__name__)


def job():
    logging.info("Test ms db job")
    try:
        
        logger.info(SERVER_INVENTORY_DB_HOST)
        logger.info(type(SERVER_INVENTORY_DB_HOST))

        conn = pymssql.connect(
            host=SERVER_INVENTORY_DB_HOST,
            user=SERVER_INVENTORY_DB_USER,
            password=SERVER_INVENTORY_DB_PASS,
            database=SERVER_INVENTORY_DB_NAME
        )
        cur = conn.cursor()

        cur.execute("SELECT * FROM DiskSpace")
        all = cur.fetchall()
        logger.info(all)
    except Exception as e:
        logger.error(e)
        return False
    return True
