# import io
# import logging
import pandas as pd
# from custom_data_connector import post_data_to_custom_data_connector
# from utils.sftp_connection import get_sftp_client
# from utils.config import SENSUM_IT_SFTP_REMOTE_SUBFOLDER_DIR
# from sensum.sensum import handle_sub_folder_files, get_subfolders

# logger = logging.getLogger(__name__)

# TODO: Fix and remove comments
# TODO: remove all commented out code
# TODO: all the merge functions could just be in the same file, maybe in sensum.py


# sftp_client = get_sftp_client()


# def get_sensum_mål():
#     try:
#         logger.info('Starting Sensum Udfører Data: Mål')
#         conn = sftp_client.get_connection()
#         subfolders = get_subfolders()

#         mål_df = handle_sub_folder_files(subfolders, conn, 'Mål_*.csv', SENSUM_IT_SFTP_REMOTE_SUBFOLDER_DIR)
#         delmål_df = handle_sub_folder_files(subfolders, conn, 'Delmål_*.csv', SENSUM_IT_SFTP_REMOTE_SUBFOLDER_DIR)
#         borger_information_df = handle_sub_folder_files(subfolders, conn, 'Borger_information_*.csv', SENSUM_IT_SFTP_REMOTE_SUBFOLDER_DIR)

#         if mål_df is not None and delmål_df is not None and borger_information_df is not None:
#             return process_files(mål_df, delmål_df, borger_information_df)
#     except Exception as e:
#         logger.error(f"An error occurred: {e}")
#         return False

#     return False

# TODO: Could this and the other merge functions not be combine into a fewer functions? / made more generic? Like merge_dataframes in sensum.py
def merge_df_sensum_mål(mål_df, delmål_df, borger_information_df):
    merged_df = pd.merge(mål_df, delmål_df, on='MålId', how='inner', suffixes=('_mål', '_delmål'))
    merged_df = pd.merge(merged_df, borger_information_df, on='BorgerId', how='inner')

    # HINT: this has been made generic in your merge_dataframes function in sensum.py
    result = merged_df.groupby('MålId').agg({
        'MålNavn': 'first',
        'Ansvarlig_mål': 'first',
        'StartDato_mål': 'first',
        'SlutDato_mål': 'first',
        'OprettetDato_mål': 'first',
        'OprettetAf_mål': 'first',
        'Lukket_mål': 'first',
        'SamarbejdeMedBorger': 'first',
        'DelmålNavn': 'first',
        'Samarbejdspartner': 'first',
        'EvalueringsDato': 'first',
        'Fornavn': 'first',
        'Efternavn': 'first',
        'CPR': 'first',
        'Afdeling': 'first'
    }).reset_index(drop=True)

    # HINT: this has been made generic in your merge_dataframes function in sensum.py
    result.columns = ['MålNavn', 'Ansvarlig', 'StartDato', 'SlutDato', 'OprettetDato', 'OprettetAf', 'Lukket',
                      'SamarbejdeMedBorger', 'DelmålNavn', 'Samarbejdspartner', 'EvalueringsDato', 'Fornavn', 'Efternavn', 'CPR', 'Afdeling']

    # HINT: this can be made generic by filtering by type and nan instead of columns names
    if pd.isna(result.at[0, 'SlutDato']):
        result.at[0, 'SlutDato'] = pd.Timestamp('1900-01-01')
    if pd.isna(result.at[0, 'Samarbejdspartner']):
        result.at[0, 'Samarbejdspartner'] = 'Ikke angivet'

    return result


# def process_files(mål_df, delmål_df, borger_information_df):
#     result = merge_df(mål_df, delmål_df, borger_information_df)

#     file = io.BytesIO(result.to_csv(index=False, sep=';').encode('utf-8'))
#     filename = "SA" + "SensumUdførerMål" + ".csv"
#     if post_data_to_custom_data_connector(filename, file):
#         logger.info("Successfully updated Sensum Mål Data")
#         return True
#     else:
#         logger.error("Failed to update Sensum Mål Data")
#         return False
