# import io
# import logging
import pandas as pd
# from custom_data_connector import post_data_to_custom_data_connector
# from utils.sftp_connection import get_sftp_client
# from utils.config import SENSUM_IT_SFTP_REMOTE_SUBFOLDER_DIR
# from sensum.sensum import handle_sub_folder_files, get_subfolders

# logger = logging.getLogger(__name__)

# sftp_client = get_sftp_client()


# def get_sensum_adminstrative_forhold():
#     try:
#         logger.info('Starting Sensum Udfører Data: Administrative forhold')
#         conn = sftp_client.get_connection()
#         subfolders = get_subfolders()

#         borger_information_df = handle_sub_folder_files(subfolders, conn, 'Borger_information_*.csv', SENSUM_IT_SFTP_REMOTE_SUBFOLDER_DIR)
#         adminstrative_forhold_df = handle_sub_folder_files(subfolders, conn, 'Administrative_*.csv', SENSUM_IT_SFTP_REMOTE_SUBFOLDER_DIR)

#         if borger_information_df is not None and adminstrative_forhold_df is not None:
#             return process_files(borger_information_df, adminstrative_forhold_df)
#     except Exception as e:
#         logger.error(f"An error occurred: {e}")
#         return False

#     return False


def merge_df_administrative_forhold(borger_information_df, adminstrative_forhold_df):
    merged_df = pd.merge(borger_information_df, adminstrative_forhold_df, on='BorgerId', how='inner')

    # HINT: this has been made generic in your merge_dataframes function in sensum.py
    result = merged_df.groupby('BorgerId').agg({
        'Fornavn': 'first',
        'Efternavn': 'first',
        'CPR': 'first',
        'Afdeling': 'first',
        'IndskrivelseDato': 'first',
        'UdskrivelsesDato': 'first',
        'HandleKommune': 'first',
        'BetalingsKommune': 'first',
        'ParagrafType': 'first',
        'IndskrivningsType': 'first',
        'PrimærAdresse': 'first',
        'PrimærPostNr': 'first',
        'PrimærBy': 'first'
    }).reset_index(drop=True)

    # HINT: this has been made generic in your merge_dataframes function in sensum.py
    result.columns = ['Fornavn', 'Efternavn', 'CPR', 'Afdeling', 'IndskrivelseDato', 'UdskrivelsesDato', 'HandleKommune',
                      'BetalingsKommune', 'ParagrafType', 'IndskrivningsType', 'PrimærAdresse',
                      'PrimærPostNr', 'PrimærBy']

    # HINT: this can be made generic by filtering by type and nan instead of columns names
    if pd.isna(result.at[0, 'UdskrivelsesDato']):
        result.at[0, 'UdskrivelsesDato'] = pd.Timestamp('1900-01-01')
    if pd.isna(result.at[0, 'HandleKommune']):
        result.at[0, 'HandleKommune'] = 'Ikke angivet'
    if pd.isna(result.at[0, 'BetalingsKommune']):
        result.at[0, 'BetalingsKommune'] = 'Ikke angivet'

    return result


# def process_files(borger_information_df, adminstrative_forhold_df):
#     result = merge_df_administrative_forhold(borger_information_df, adminstrative_forhold_df)

#     file = io.BytesIO(result.to_csv(index=False, sep=';').encode('utf-8'))
#     filename = "SA" + "SensumUdførerAdminstrativeForhold" + ".csv"
#     if post_data_to_custom_data_connector(filename, file):
#         logger.info("Successfully updated Sensum Administrative forhold Data")
#         return True
#     else:
#         logger.error("Failed to update Sensum Administrative forhold Data")
#         return False
