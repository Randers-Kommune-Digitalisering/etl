# import io
# import logging
import pandas as pd
# from custom_data_connector import post_data_to_custom_data_connector
# from utils.sftp_connection import get_sftp_client
# from utils.config import SENSUM_IT_SFTP_REMOTE_DIR
# from sensum.sensum import handle_sub_folder_files, get_subfolders

# logger = logging.getLogger(__name__)

# TODO: Fix and remove comments
# TODO: remove all commented out code
# TODO: all the merge functions could just be in the same file, maybe in sensum.py

# sftp_client = get_sftp_client()


# def get_sensum_ydelse():
#     try:
#         logger.info('Starting Sensum Udfører Data: Ydelse')
#         conn = sftp_client.get_connection()
#         subfolders = get_subfolders()

#         ydelse_df = handle_sub_folder_files(subfolders, conn, 'Ydelse_*.csv', SENSUM_IT_SFTP_REMOTE_DIR)
#         borger_information_df = handle_sub_folder_files(subfolders, conn, 'Borger_information_*.csv', SENSUM_IT_SFTP_REMOTE_DIR)
#         afdeling_df = handle_sub_folder_files(subfolders, conn, 'Afdeling_*.csv', SENSUM_IT_SFTP_REMOTE_DIR)

#         if ydelse_df is not None and borger_information_df is not None and afdeling_df is not None:
#             return process_files(ydelse_df, borger_information_df, afdeling_df)
#     except Exception as e:
#         logger.error(f"An error occurred: {e}")
#         return False

#     return False

# TODO: Could this and the other merge functions not be combine into a fewer functions? / made more generic? Like merge_dataframes in sensum.py
def merge_df_ydelse(ydelse_df, borger_information_df, afdeling_df):
    ydelse_df = pd.merge(ydelse_df, afdeling_df[['AfdelingId', 'Navn']], on='AfdelingId', how='left')
    merged_df = pd.merge(ydelse_df, borger_information_df, on='BorgerId', how='inner')

    # HINT: this has been made generic in your merge_dataframes function in sensum.py
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

    # HINT: this has been made generic in your merge_dataframes function in sensum.py
    result.columns = ['YdelseNavn', 'StartDato', 'SlutDato', 'CPR', 'Fornavn', 'Efternavn', 'AfdelingNavn', 'Afdeling']
    
    # HINT: maybe this can be made generic by filtering by type and nan instead of columns names
    if pd.isna(result.at[0, 'CPR']):
        result.at[0, 'CPR'] = 000000000.0

    return result


# def process_files(ydelse_df, borger_information_df, afdeling_df):
#     result = merge_df_ydelse(ydelse_df, borger_information_df, afdeling_df)

#     file = io.BytesIO(result.to_csv(index=False, sep=';').encode('utf-8'))
#     filename = "SA" + "SensumUdførerYdelse" + ".csv"
#     if post_data_to_custom_data_connector(filename, file):
#         logger.info("Successfully updated Sensum Ydelse Data")
#         return True
#     else:
#         logger.error("Failed to update Sensum Ydelse Data")
#         return False
