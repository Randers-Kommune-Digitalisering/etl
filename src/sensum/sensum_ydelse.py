import io
import logging
import os
import pandas as pd
import fnmatch
from custom_data_connector import post_data_to_custom_data_connector
from utils.sftp_connection import get_sftp_client
from utils.config import SENSUM_IT_SFTP_REMOTE_SUBFOLDER_DIR

logger = logging.getLogger(__name__)

sftp_client = get_sftp_client()


def get_sensum_ydelse():
    try:
        logger.info('Starting Sensum Udfører Data: Ydelse')
        conn = sftp_client.get_connection()
        subfolders = ['Baa', 'BeVej', 'BoAu', 'Born_Bo', 'Bvh', 'Frem', 'Hjorne', 'Job', 'Lade', 'Lbg', 'Meau', 'Mepu',
                      'P4', 'Phus', 'Psyk', 'Senhj', 'STU']

        ydelse_df = handle_files(subfolders, conn, 'Ydelse_*.csv')
        borger_information_df = handle_files(subfolders, conn, 'Borger_information_*.csv')
        afdeling_df = handle_files(subfolders, conn, 'Afdeling_*.csv')

        if ydelse_df is not None and borger_information_df is not None and afdeling_df is not None:
            return process_files(ydelse_df, borger_information_df, afdeling_df)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False

    return False


def handle_files(subfolders, connection, file_pattern):
    try:
        df_list = []
        for subfolder in subfolders:
            folder_path = os.path.join(SENSUM_IT_SFTP_REMOTE_SUBFOLDER_DIR, subfolder)
            files = [f for f in connection.listdir(folder_path) if fnmatch.fnmatch(f, file_pattern)]

            if not files:
                logger.info(f"No files found in {subfolder} matching pattern {file_pattern}")
                continue

            for file in files:
                logger.info(f"Found file in {subfolder}: {file}")

            latest_file = max(files, key=lambda f: connection.stat(os.path.join(folder_path, f)).st_mtime)
            logger.info(f"Latest file in {subfolder}: {latest_file}")

            with connection.open(os.path.join(folder_path, latest_file).replace("\\", "/")) as f:
                df = pd.read_csv(f, sep=";", header=0, decimal=",")
                df_list.append(df)

        if df_list:
            df = pd.concat(df_list, ignore_index=True)
            df = df.drop_duplicates()
            return df
        else:
            logger.info("No files found in the specified subfolders.")
            return None

    except Exception as e:
        logger.error(f"Error handling files: {e}")
        return None


def merge_df(ydelse_df, borger_information_df, afdeling_df):
    ydelse_df = pd.merge(ydelse_df, afdeling_df[['AfdelingId', 'Navn']], on='AfdelingId', how='left')
    merged_df = pd.merge(ydelse_df, borger_information_df, on='BorgerId', how='inner')

    result = merged_df.groupby('YdelseId').agg({
        'YdelseNavn': 'first',
        'StartDato': 'first',
        'SlutDato': 'first',
        'CPR': 'first',
        'Fornavn': 'first',
        'Efternavn': 'first',
        'Navn': 'first',
        'Afdeling': 'first',
    }).reset_index(drop=True)

    result.columns = ['YdelseNavn', 'StartDato', 'SlutDato', 'CPR', 'Fornavn', 'Efternavn', 'AfdelingNavn', 'Afdeling']

    if pd.isna(result.at[0, 'CPR']):
        result.at[0, 'CPR'] = 000000000.0

    return result


def process_files(ydelse_df, borger_information_df, afdeling_df):
    result = merge_df(ydelse_df, borger_information_df, afdeling_df)

    file = io.BytesIO(result.to_csv(index=False, sep=';').encode('utf-8'))
    filename = "SA" + "SensumUdførerYdelsev" + ".csv"
    if post_data_to_custom_data_connector(filename, file):
        logger.info("Successfully updated Sensum Ydelse Data")
        return True
    else:
        logger.error("Failed to update Sensum Ydelse Data")
        return False
