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


def get_sensum_medarbejder():
    try:
        logger.info('Starting Sensum Medarbejder and Afdeling')
        sftp_client = SFTPClient(SENSUM_IT_SFTP_HOST, SENSUM_IT_SFTP_USER, password=SENSUM_IT_SFTP_PASS)
        conn = sftp_client.get_connection()
        afdeling_files = get_files(conn, 'Afdeling_*.csv')
        medarbejder_files = get_files(conn, 'Medarbejder_*.csv')

        if afdeling_files and medarbejder_files:
            return process_files(afdeling_files, medarbejder_files, conn)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False

    return False


def handle_afdeling_files(files, connection):
    logger.info('Handling Sensum Afdeling files')

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
        df = df.drop_duplicates()
        return df

    except Exception as e:
        logger.error(f"Error handling Afdeling files: {e}")
        return None


def handle_medarbejder_files(files, connection):
    logger.info('Handling Sensum Medarbejder files')

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
        df = df.drop_duplicates()
        return df

    except Exception as e:
        logger.error(f"Error handling Medarbejder files: {e}")
        return None


def merge_df(afdeling_df, medarbejder_df):
    merged_df = pd.merge(afdeling_df, medarbejder_df, on='AfdelingId', how='inner')
    result = merged_df.groupby('MedarbejderId').agg({
        'Fornavn': 'first',
        'Efternavn': 'first',
        'Navn': 'first',
        'Aktiv': 'first',
        'StartDato': 'first',
        'SlutDato': 'first',


    }).reset_index(drop=True)

    result.columns = ['MedarbejderFornavn', 'MedarbejderEfternavn', 'AfdelingNavn', 'Aktiv', 'StartDato', 'SlutDato']

    return result


def get_files(conn, pattern):
    return [f for f in conn.listdir(SENSUM_IT_SFTP_REMOTE_DIR) if fnmatch.fnmatch(f, pattern)]


def process_files(afdeling_files, medarbejder_files, conn):
    afdeling_df = handle_afdeling_files(afdeling_files, conn)
    medarbejder_df = handle_medarbejder_files(medarbejder_files, conn)

    if afdeling_df is not None and medarbejder_df is not None:
        result = merge_df(afdeling_df, medarbejder_df)

        file = io.BytesIO(result.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "SA" + "SensumMedarbejder" + ".csv"
        if post_data_to_custom_data_connector(filename, file):
            logger.info("Successfully updated Sensum Medarbejder")
            return True
        else:
            logger.error("Failed to update Sensum Medarbejder")
            return False
    return False
