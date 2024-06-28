import pymssql
import pandas as pd
import logging
from utils.config import FRONTDESK_DB_USER, FRONTDESK_DB_PASS, FRONTDESK_DB_HOST, FRONTDESK_DB_DATABASE

logger = logging.getLogger(__name__)

def connectToFrontdeskDB():
    conn = pymssql.connect(FRONTDESK_DB_HOST, FRONTDESK_DB_USER, FRONTDESK_DB_PASS, FRONTDESK_DB_DATABASE)
    cursor = conn.cursor()
    # cursor.execute("SELECT * FROM information_schema.tables")
    # rows = cursor.fetchall()
    # logger.info(f'Tabeller i databasen ({FRONTDESK_DB_DATABASE}): {rows}')

    # cursor.execute("SELECT * FROM information_schema.columns where table_name='Ticket'")
    # rows = cursor.fetchall()
    # logger.info(f'Kolonner i Ticket: {rows}')

    # Hente alle data ned lokalt til udvikling 
    # tables=["Operation","QueueLocation","Registration","Reservation","ResourceAvailability","ResourceReserved","Ticket"]
    tables=["Operation"]
    for table in tables:
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        logger.info(cursor.description)
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)
        #df.to_csv(f"src/frontdesk/data/{table}.csv", index=False)

    return conn, df



