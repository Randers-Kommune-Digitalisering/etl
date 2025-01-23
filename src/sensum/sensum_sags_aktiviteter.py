# import io
# import logging
import pandas as pd
# from custom_data_connector import post_data_to_custom_data_connector
# from sensum.sensum import handle_files, get_files
# from utils.sftp_connection import get_sftp_client
# from utils.config import SENSUM_IT_SFTP_REMOTE_DIR
# logger = logging.getLogger(__name__)


# TODO: Fix and remove comments
# TODO: remove all commented out code
# TODO: all the merge functions could just be in the same file, maybe in sensum.py

# sftp_client = get_sftp_client()


# def get_sensum_sags_aktiviteter():
#     try:
#         logger.info('Starting Sensum Sags Aktiviteter')
#         conn = sftp_client.get_connection()
#         sager_files = get_files(conn, directory=SENSUM_IT_SFTP_REMOTE_DIR, subdirectory='sensum_randers',  pattern='Sager_*.csv')
#         sags_aktivitet_files = get_files(conn, directory=SENSUM_IT_SFTP_REMOTE_DIR, subdirectory='sensum_randers', pattern='SagsAktiviteter_*.csv')
#         borger_files = get_files(conn, directory=SENSUM_IT_SFTP_REMOTE_DIR, subdirectory='sensum_randers', pattern='Borger_*.csv')

#         if sager_files and sags_aktivitet_files and borger_files:
#             return process_files(sager_files, sags_aktivitet_files, borger_files, conn)
#     except Exception as e:
#         logger.error(f"An error occurred: {e}")
#         return False

#     return False

# TODO: Could this and the other merge functions not be combine into a fewer functions? / made more generic? Like merge_dataframes in sensum.py
def sags_aktiviteter_merge_df(sager_df, sags_aktivitet_df, borger_df):
    sager_df = sager_df.rename(columns={'SagModel': 'Sager_SagModel'})

    merged_df = pd.merge(sager_df, sags_aktivitet_df, on='SagId', how='inner')
    merged_df = pd.merge(merged_df, borger_df[['BorgerId', 'CPR', 'Fornavn', 'Efternavn']], on='BorgerId', how='left')

    # HINT: this has been made generic in your merge_dataframes function in sensum.py
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

    # HINT: this has been made generic in your merge_dataframes function in sensum.py
    result.columns = ['Counter', 'SagModel', 'FaseNavn', 'AktivitetNavn', 'AktivitetSvar', 'Deadline', 'UdførtDato', 'CPR', 'Fornavn', 'Efternavn',
                      'AfdelingNavn', 'PrimærAnsvarlig']

    # HINT: this can be made generic by filtering by type and nan instead of columns names
    if pd.isna(result.at[0, 'AktivitetSvar']):
        result.at[0, 'AktivitetSvar'] = pd.Timestamp('1900-01-01')
    if pd.isna(result.at[0, 'Deadline']):
        result.at[0, 'Deadline'] = pd.Timestamp('1900-01-01')
    if pd.isna(result.at[0, 'UdførtDato']):
        result.at[0, 'UdførtDato'] = pd.Timestamp('1900-01-01')

    return result


# def process_files(sager_files, sags_aktivitet_files, borger_files, conn):
#     sager_df = handle_files(sager_files, conn)
#     sags_aktivitet_df = handle_files(sags_aktivitet_files, conn)
#     borger_df = handle_files(borger_files, conn)

#     if sager_df is not None and sags_aktivitet_df is not None and borger_df is not None:
#         result = sags_aktiviteter_merge_df(sager_df, sags_aktivitet_df, borger_df)

#         file = io.BytesIO(result.to_csv(index=False, sep=';').encode('utf-8'))
#         filename = "SA" + "SensumSagsAktivitet" + ".csv"  # Shouldn't it be "SA_SensumSagsAktivitet.csv"?!
#         if post_data_to_custom_data_connector(filename, file):
#             logger.info("Successfully updated Sensum Sags Aktiviteter")
#             return True
#         else:
#             logger.error("Failed to update Sensum Sags Aktiviteter")
#             return False
#     return False
