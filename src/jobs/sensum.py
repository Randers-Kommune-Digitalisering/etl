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


def job():
    sftp_client = SFTPClient(SENSUM_IT_SFTP_HOST, SENSUM_IT_SFTP_USER, password=SENSUM_IT_SFTP_PASS)
    conn = sftp_client.get_connection()
    filelist = [f for f in conn.listdir(SENSUM_IT_SFTP_REMOTE_DIR) if fnmatch.fnmatch(f, 'Sager_*.csv')]
    if filelist and conn:
        return handle_files(filelist, conn)
    return False


def handle_files(files, connection):
    logger.info('Handling Sensum IT files')

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
                needed_cols = [
                    'SagId', 'SagNavn', 'SagType', 'SagModel', 'BorgerId', 'AfdelingId',
                    'AfdelingNavn', 'PrimærAnsvarlig', 'AlternativSagsbehandler',
                    'AlternativTeam', 'ForventetParagraf', 'Status', 'Akut', 'HandleKommune',
                    'BetalingsKommune', 'SagsbehandlerBetalingsKommune', 'HenvendelsesDato',
                    'AnsøgningModtagetDato', 'SlutDato', 'AfslutningsÅrsag', 'Facet', 'JournalKode'
                ]

                df = pd.read_csv(f, sep=";", header=0, decimal=",", usecols=needed_cols)
                # Filling missing values in columns with the first row's value
                if pd.isna(df.at[0, 'SlutDato']):
                    df.at[0, 'SlutDato'] = "Missing Value"
                if pd.isna(df.at[0, 'AlternativTeam']):
                    df.at[0, 'AlternativTeam'] = "Missing Value"
                if pd.isna(df.at[0, 'AlternativSagsbehandler']):
                    df.at[0, 'AlternativSagsbehandler'] = "Missing Value"
                if pd.isna(df.at[0, 'SagsbehandlerBetalingsKommune']):
                    df.at[0, 'SagsbehandlerBetalingsKommune'] = "Missing Value"
                if pd.isna(df.at[0, 'AfslutningsÅrsag']):
                    df.at[0, 'AfslutningsÅrsag'] = "Missing Value"

                df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)

        file = io.BytesIO(df.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "Sager" + ".csv"

        if post_data_to_custom_data_connector(filename, file):
            logger.info("Successfully updated Sager")
            return True
        else:
            logger.error("Failed to update Sager")
            return False

        # if df_list:
        #     df = pd.concat(df_list, ignore_index=True)
        #     df_to_csv(df, 'Sager')
        #     return df
        # else:
        #     logger.error("No data frames to concatenate.")
        #     return None

    except Exception as e:
        logger.error(f"Error handling files: {e}")
        return None


def df_to_csv(df, filename):  # For testing locally purposes
    try:
        df.to_csv(f"{filename}.csv", index=False)
        logger.info(f"DataFrame saved to {filename}.csv")
    except Exception as e:
        logger.error(f"Error saving DataFrame to CSV: {e}")
