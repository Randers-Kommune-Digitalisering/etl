import io
import logging
import pandas as pd
from custom_data_connector import post_data_to_custom_data_connector
from utils.sftp_connection import get_sftp_client
from utils.config import SENSUM_IT_SFTP_REMOTE_SUBFOLDER_DIR
from sensum.sensum import handle_sub_folder_files, get_subfolders

logger = logging.getLogger(__name__)

sftp_client = get_sftp_client()


def get_sensum_ydelse():
    try:
        logger.info('Starting Sensum Udfører Data: Ydelse')
        conn = sftp_client.get_connection()
        subfolders = get_subfolders()

        ydelse_df = handle_sub_folder_files(subfolders, conn, 'Ydelse_*.csv', SENSUM_IT_SFTP_REMOTE_SUBFOLDER_DIR)
        borger_information_df = handle_sub_folder_files(subfolders, conn, 'Borger_information_*.csv', SENSUM_IT_SFTP_REMOTE_SUBFOLDER_DIR)
        afdeling_df = handle_sub_folder_files(subfolders, conn, 'Afdeling_*.csv', SENSUM_IT_SFTP_REMOTE_SUBFOLDER_DIR)

        if ydelse_df is not None and borger_information_df is not None and afdeling_df is not None:
            return process_files(ydelse_df, borger_information_df, afdeling_df)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False

    return False


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
    filename = "SA" + "SensumUdførerYdelse" + ".csv"
    if post_data_to_custom_data_connector(filename, file):
        logger.info("Successfully updated Sensum Ydelse Data")
        return True
    else:
        logger.error("Failed to update Sensum Ydelse Data")
        return False
