import pymssql
import pandas as pd
import logging
# import io
# import os
# from prophet import Prophet
from datetime import datetime
from utils.config import FRONTDESK_DB_USER, FRONTDESK_DB_PASS, FRONTDESK_DB_HOST, FRONTDESK_DB_DATABASE
from utils.database_connection import get_db_frontdesk

logger = logging.getLogger(__name__)

db_client = get_db_frontdesk()


def job():
    logger.info("Initializing Frontdesk j obcenter job")

    try:
        workdata = connectToFrontdeskDB()
        # logger.info(os.getcwd())
        # workdata = pd.read_csv('C:\\Users\\dqa8511\\Desktop\\Github\\etl\\src\\jobs\\data\\FrontdeskBorgerserviceTables.csv', sep=';')

    except Exception as e:
        logger.error(e)
        logger.error("Failed to connect to Frontdesk database")
        return False
    else:
        logger.info("Connected to Frontdesk database successfully")

        # logger.info(workdata)
        # Transformations
        workdata = transformations(workdata)

    # Upload operations to PostgreSQL
    try:
        db_client.ensure_database_exists()
        connection = db_client.get_connection()
        if connection:
            logger.info("Attempting to upload Frontdesk Jobcenter operations to PostgreSQL")
            workdata.to_sql('operationsjobcenter', con=connection, if_exists='replace', index=False)
            logger.info("Updated Frontdesk Jobcenter operations successfully in PostgreSQL")
            logger.info(f"Frontdesk Jobcenter operations columns: {workdata.columns.tolist()}")
            connection.close()
        else:
            logger.error("Failed to connect to PostgreSQL")
            return False
    except Exception as e:
        logger.error(e)
        logger.error("Failed to update Frontdesk Jobcenter operations in PostgreSQL")
        return False

    return True


def transformations(data):

    # Vælger kun data fra Borgerservice
    data = data[data['QueueName'] == 'Jobcenter']

    data['CreatedAt'] = pd.to_datetime(data['CreatedAt']).dt.tz_localize(None)
    data['CalledAt'] = pd.to_datetime(data['CalledAt']).dt.tz_localize(None)
    data['EndedAt'] = pd.to_datetime(data['EndedAt']).dt.tz_localize(None)
    data['LastAggregatedDataUpdateTime'] = pd.to_datetime(data['LastAggregatedDataUpdateTime']).dt.tz_localize(None)

    dateTwoYearsBefore = datetime(datetime.now().year - 2, datetime.now().month, datetime.now().day)
    data.drop(data[(data.CreatedAt < datetime(2023, 1, 1)) | (data.CreatedAt < dateTwoYearsBefore)].index, inplace=True)

    data['dato'] = data['CreatedAt'].dt.date
    data['ugenr'] = data['CreatedAt'].dt.isocalendar().week
    data['år'] = data['CreatedAt'].dt.year

    # Drop unnecessary columns and filter rows based on 'State'
    data = data[data['State'] != "Discarded"]
    data = data.drop(columns=['QueueId', 'QueueName', 'MunicipalityID', 'DelayedUntil', 'DelayedFrom', 'IsEmployeeAnonymized', 'QueueCategoryId', 'StateId', 'State'])

    # Gemmer data midlertidigt til CSV-fil
    # data.to_csv('C:\\Users\\dqa8511\\Desktop\\Github\\etl\\src\\jobs\\data\\FrontdeskJobcenter.csv', sep=',', index=False)
    # logger.info(data)

    return data


def connectToFrontdeskDB():
    conn = pymssql.connect(FRONTDESK_DB_HOST, FRONTDESK_DB_USER, FRONTDESK_DB_PASS, FRONTDESK_DB_DATABASE)
    cursor = conn.cursor()

    tables = ["Operation"]
    for table in tables:
        cursor.execute(f"SELECT * FROM {table}")
        rows = cursor.fetchall()
        # logger.info(cursor.description)
        columns = [desc[0] for desc in cursor.description]
        df = pd.DataFrame(rows, columns=columns)

    return df
