import io
import logging
import pandas as pd
from custom_data_connector import post_data_to_custom_data_connector
from sensum.sensum import handle_files, get_files
from utils.sftp_connection import get_sftp_client

logger = logging.getLogger(__name__)

sftp_client = get_sftp_client()


def get_sensum_data():
    try:
        logger.info('Starting Sensum Data')
        conn = sftp_client.get_connection()
        sager_files = get_files(conn, 'Sager_*.csv')
        indsatser_files = get_files(conn, 'Indsatser_*.csv')
        borger_files = get_files(conn, 'Borger_*.csv')

        if sager_files and indsatser_files and borger_files:
            return process_files(sager_files, indsatser_files, borger_files, conn)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False

    return False


def merge_df(sager_df, indsatser_df, borger_df):
    merged_df = pd.merge(indsatser_df, sager_df, on='SagId', how='inner')
    merged_df = pd.merge(merged_df, borger_df, on='BorgerId', how='inner')
    result = merged_df.groupby('IndsatsId').agg({
        'BorgerId': 'nunique',
        'IndsatsStatus': 'first',
        'Indsats': 'first',
        'CPR': 'first',
        'Fornavn': 'first',
        'Efternavn': 'first',
        'IndsatsStartDato': 'first',
        'IndsatsSlutDato': 'first',
        'OprettetDato': 'first',
        'OpholdsKommune': 'first',
        'SagNavn': 'first',
        'SagType': 'first',
        'Status': 'first',
        'PrimærAnsvarlig': 'first',
        'Akut': 'first',
        'AfslutningsÅrsag': 'first',
        'LeverandørIndsats': 'first',
        'PrimærBy': 'first',
        'LeverandørNavn': 'first',
        'Primær målgruppe': 'first',
        'Sekundær målgruppe': 'first',

    }).reset_index(drop=True)

    result.columns = ['Counter', 'IndsatsStatus', 'Indsats', 'CPR', 'Fornavn', 'Efternavn', 'IndsatsStartDato', 'IndsatsSlutDato',
                      'OprettetDato', 'OpholdsKommune', 'SagNavn', 'SagType', 'Status',
                      'PrimærAnsvarlig', 'Akut', 'AfslutningsÅrsag', 'LeverandørIndsats', 'PrimærBy', 'LeverandørNavn',
                      'Primær målgruppe', 'Sekundær målgruppe']

    if pd.isna(result.at[0, 'OprettetDato']):
        result.at[0, 'OprettetDato'] = pd.Timestamp('1900-01-01')
    if pd.isna(result.at[0, 'Sekundær målgruppe']):
        result.at[0, 'Sekundær målgruppe'] = 'Ikke angivet'

    result['OprettetDato'] = pd.to_datetime(result['OprettetDato'])

    return result


def process_files(sager_files, indsatser_files, borger_files, conn):
    sager_df = handle_files(sager_files, conn)
    indsatser_df = handle_files(indsatser_files, conn)
    borger_df = handle_files(borger_files, conn)

    if sager_df is not None and indsatser_df is not None and borger_df is not None:
        result = merge_df(sager_df, indsatser_df, borger_df)

        file = io.BytesIO(result.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "SA" + "Sensum" + ".csv"
        if post_data_to_custom_data_connector(filename, file):
            logger.info("Successfully updated Sensum Data")
            return True
        else:
            logger.error("Failed to update Sensum Data")
            return False
    return False
