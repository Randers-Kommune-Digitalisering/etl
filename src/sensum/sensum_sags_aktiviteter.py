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


def get_sensum_sags_aktiviteter():
    try:
        logger.info('Starting Sensum Sager')
        sftp_client = SFTPClient(SENSUM_IT_SFTP_HOST, SENSUM_IT_SFTP_USER, password=SENSUM_IT_SFTP_PASS)
        conn = sftp_client.get_connection()
        sager_files = get_files(conn, 'Sager_*.csv')
        sags_aktivitet_files = get_files(conn, 'SagsAktiviteter_*.csv')
        borger_files = get_files(conn, 'Borger_*.csv')

        if sager_files and sags_aktivitet_files and borger_files:
            return process_files(sager_files, sags_aktivitet_files, borger_files, conn)
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
        df = df.drop_duplicates()
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
        df = df.drop_duplicates()
        return df

    except Exception as e:
        logger.error(f"Error handling Borger files: {e}")
        return None


def handle_sags_aktiviteter_files(files, connection):
    logger.info('Handling Sensum Sags Aktiviteter files')

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
        logger.error(f"Error handling Sags Aktiviteter files: {e}")
        return None


def merge_df(sager_df, sags_aktivitet_df, borger_df):
    sager_df = sager_df.rename(columns={'SagModel': 'Sager_SagModel'})

    merged_df = pd.merge(sager_df, sags_aktivitet_df, on='SagId', how='inner')
    merged_df = pd.merge(merged_df, borger_df[['BorgerId', 'CPR', 'Fornavn', 'Efternavn']], on='BorgerId', how='left')

    result = merged_df.groupby('SagAktivitetId').agg({
        'BorgerId': 'nunique',
        'SagModel': 'first',
        'FaseNavn': 'first',
        'AktivitetNavn': 'first',
        'AktivitetSvar': 'first',
        'Deadline': 'first',
        'UdførtDato': 'first',
        'CPR': 'first',
        'Fornavn': 'first',
        'Efternavn': 'first',
        'AfdelingNavn': 'first',
        'PrimærAnsvarlig': 'first',
    }).reset_index(drop=True)

    result.columns = ['Counter', 'SagModel', 'FaseNavn', 'AktivitetNavn', 'AktivitetSvar', 'Deadline', 'UdførtDato', 'CPR', 'Fornavn', 'Efternavn',
                      'AfdelingNavn', 'PrimærAnsvarlig']

    if pd.isna(result.at[0, 'AktivitetSvar']):
        result.at[0, 'AktivitetSvar'] = pd.Timestamp('1900-01-01')
    if pd.isna(result.at[0, 'Deadline']):
        result.at[0, 'Deadline'] = pd.Timestamp('1900-01-01')
    if pd.isna(result.at[0, 'UdførtDato']):
        result.at[0, 'UdførtDato'] = pd.Timestamp('1900-01-01')

    return result


def get_files(conn, pattern):
    return [f for f in conn.listdir(SENSUM_IT_SFTP_REMOTE_DIR) if fnmatch.fnmatch(f, pattern)]


def process_files(sager_files, sags_aktivitet_files, borger_files, conn):
    sager_df = handle_sager_files(sager_files, conn)
    sags_aktivitet_df = handle_sags_aktiviteter_files(sags_aktivitet_files, conn)
    borger_df = handle_borger_files(borger_files, conn)

    if sager_df is not None and sags_aktivitet_df is not None and borger_df is not None:
        result = merge_df(sager_df, sags_aktivitet_df, borger_df)

        file = io.BytesIO(result.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "SA" + "SensumSagsAktivitet" + ".csv"
        if post_data_to_custom_data_connector(filename, file):
            logger.info("Successfully updated Sensum Sags Aktiviteter")
            return True
        else:
            logger.error("Failed to update Sensum Sags Aktiviteter")
            return False
    return False
