import os
import io
import fnmatch
import logging
import pandas as pd
from datetime import datetime, timedelta
from utils.config import SENSUM_IT_SFTP_HOST, SENSUM_IT_SFTP_USER, SENSUM_IT_SFTP_PASS, SENSUM_IT_SFTP_REMOTE_DIR
from utils.stfp import SFTPClient
from custom_data_connector import post_data_to_custom_data_connector

logger = logging.getLogger(__name__)


def get_sensum_sager():
    try:
        logger.info('Starting Sensum Sager')
        sftp_client = SFTPClient(SENSUM_IT_SFTP_HOST, SENSUM_IT_SFTP_USER, password=SENSUM_IT_SFTP_PASS)
        conn = sftp_client.get_connection()
        sager_files = get_files(conn, 'Sager_*.csv')
        borger_files = get_files(conn, 'Borger_*.csv')

        if sager_files and borger_files:
            return process_files(sager_files, borger_files, conn)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False

    return False


def handle_sager_files(files, connection):
    logger.info('Handling Sensum Sager files')

    try:
        latest_file = max(files, key=lambda f: connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, f)).st_mtime)
        logger.info(f"Latest file: {latest_file}")

        last_modified_time = connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, latest_file)).st_mtime
        date = datetime.fromtimestamp(last_modified_time)

        max_date = date - timedelta(days=1)
        min_date = datetime(date.year - 2, date.month, 1)

        logger.info(f'Data periode: {min_date} - {max_date}')

        files = [f for f in files if datetime.fromtimestamp(connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, f)).st_mtime) >= min_date]

        df_list = []

        min_date = pd.to_datetime(min_date)

        for filename in files:
            with connection.open(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, filename).replace("\\", "/")) as f:

                df = pd.read_csv(f, sep=";", header=0, decimal=",")

                df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)
        return df

    except Exception as e:
        logger.error(f"Error handling files: {e}")
        return None


def handle_borger_files(files, connection):
    logger.info('Handling Sensum Borger files')

    try:
        latest_file = max(files, key=lambda f: connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, f)).st_mtime)
        logger.info(f"Latest file: {latest_file}")

        last_modified_time = connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, latest_file)).st_mtime
        date = datetime.fromtimestamp(last_modified_time)

        max_date = date - timedelta(days=1)
        min_date = datetime(date.year - 2, date.month, 1)

        logger.info(f"Data periode: {min_date} - {max_date}")

        files = [f for f in files if datetime.fromtimestamp(connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, f)).st_mtime) >= min_date]

        df_list = []

        min_date = pd.to_datetime(min_date)

        for filename in files:
            with connection.open(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, filename).replace("\\", "/")) as f:

                df = pd.read_csv(f, sep=";", header=0, decimal=",")

                df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)
        return df

    except Exception as e:
        logger.error(f"Error handling Borger files: {e}")
        return None


def merge_df(sager_df, borger_df):
    merged_df = pd.merge(sager_df, borger_df, on='BorgerId', how='inner')
    result = merged_df.groupby('SagId').agg({
        'BorgerId': 'nunique',
        'SagNavn': 'first',
        'SagType': 'first',
        'SagModel': 'first',
        'CPR': 'first',
        'Fornavn': 'first',
        'Efternavn': 'first',
        'AfdelingNavn': 'first',
        'PrimærAnsvarlig': 'first',
        'ForventetParagraf': 'first',
        'Status': 'first',
        'HandleKommune': 'first',
        'BetalingsKommune': 'first',
        'HenvendelsesDato': 'first',
        'SlutDato': 'first',


    }).reset_index(drop=True)

    result.columns = ['Counter', 'SagNavn', 'SagType', 'SagModel', 'CPR', 'Fornavn', 'Efternavn', 'AfdelingNavn', 'PrimærAnsvarlig',
                      'ForventetParagraf', 'Status', 'HandleKommune', 'BetalingsKommune', 'HenvendelsesDato',
                      'SlutDato']

    return result


def get_files(conn, pattern):
    return [f for f in conn.listdir(SENSUM_IT_SFTP_REMOTE_DIR) if fnmatch.fnmatch(f, pattern)]


def process_files(sager_files, borger_files, conn):
    sager_df = handle_sager_files(sager_files, conn)
    borger_df = handle_sager_files(borger_files, conn)

    if sager_df is not None:
        result = merge_df(sager_df, borger_df)

        file = io.BytesIO(result.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "SA" + "SensumSager" + ".csv"
        if post_data_to_custom_data_connector(filename, file):
            logger.info("Successfully updated Sensum Sager")
            return True
        else:
            logger.error("Failed to update Sensum Sager")
            return False
    return False
