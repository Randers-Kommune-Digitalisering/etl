import os
import io
import fnmatch
import logging
import pandas as pd
from datetime import datetime, timedelta
from utils.config import SENSUM_IT_SFTP_REMOTE_DIR
from utils.sftp_connection import get_sftp_client
from custom_data_connector import post_data_to_custom_data_connector

logger = logging.getLogger(__name__)

sftp_client = get_sftp_client()


def process_sensum(file_patterns, directories, process_func, merge_func, output_filename):
    try:
        logger.info(f'Starting {output_filename}')
        with sftp_client.get_connection() as conn:
            if directories:
                file_list_list = []
                for pattern in file_patterns:
                    files_list = []
                    for dir in directories:
                        files_list += get_files(connection=conn, directory=SENSUM_IT_SFTP_REMOTE_DIR, subdirectory=dir, pattern=pattern)
                    if files_list:
                        file_list_list.append(files_list)
                    else:
                        raise Exception(f"No files found for pattern {pattern}")

                if all(file_list_list):
                    return process_and_post_files(file_list_list, conn, process_func, merge_func, output_filename)
            else:
                files_list = [get_files(connection=conn, directory=SENSUM_IT_SFTP_REMOTE_DIR, subdirectory='sensum_randers', pattern=pattern) for pattern in file_patterns]

                if all(files_list):
                    return process_and_post_files(files_list, conn, process_func, merge_func, output_filename)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False

    return False


def handle_files(files, connection):
    try:
        latest_file = max(files, key=lambda f: connection.stat(f).st_mtime)
        logger.info(f"Latest file: {os.path.basename(latest_file)}")

        last_modified_time = connection.stat(latest_file).st_mtime
        date = datetime.fromtimestamp(last_modified_time)

        max_date = date - timedelta(days=1)
        min_date = datetime(date.year - 2, date.month, 1)

        logger.info(f'Data period: {min_date} - {max_date}')

        files = [f for f in files if datetime.fromtimestamp(connection.stat(f).st_mtime) >= min_date]

        df_list = []
        min_date = pd.to_datetime(min_date)

        for filename in files:
            with connection.open(filename) as f:
                df = pd.read_csv(f, sep=";", header=0, decimal=",")
                df_list.append(df)

        if df_list:
            df = pd.concat(df_list, ignore_index=True)
            df = df.drop_duplicates()
            return df
        else:
            logger.info("No files found.")
            return None

    except Exception as e:
        logger.error(f"Error handling files: {e}")
        return None


def merge_dataframes(df1, df2, merge_on, group_by, agg_dict, columns):
    try:
        merged_df = pd.merge(df1, df2, on=merge_on, how='inner')
        result = merged_df.groupby(group_by).agg(agg_dict).reset_index(drop=True)
        result.columns = columns

        for col in result.columns:
            if pd.isna(result.at[0, col]):
                if result[col].dtype == 'datetime64[ns]':
                    result.at[0, col] = pd.Timestamp('1900-01-01')
                elif result[col].dtype == 'object':
                    result.at[0, col] = 'Ikke angivet'
                elif result[col].dtype == 'float64' or result[col].dtype == 'int64':
                    result.at[0, col] = 0
    except Exception as e:
        raise Exception(f"An error occurred: {e}")
    return result


def get_files(connection, directory, subdirectory, pattern, only_latest=False):
    try:
        if only_latest:
            latest_file = max([os.path.join(directory, subdirectory, f) for f in connection.listdir(os.path.join(directory, subdirectory)) if fnmatch.fnmatch(f, pattern)], key=lambda f: connection.stat(f).st_mtime, default=None)
            if latest_file:
                return [latest_file]
            return []
        return [os.path.join(directory, subdirectory, f) for f in connection.listdir(os.path.join(directory, subdirectory)) if fnmatch.fnmatch(f, pattern)]
    except Exception as e:
        raise Exception(f"An error occurred while getting files: {e}")


def process_and_post_files(file_list_list, conn, process_func, merge_func, output_filename):
    dfs = [process_func(file_list, conn) for file_list in file_list_list]

    if all(df is not None and not df.empty for df in dfs):
        result = merge_func(*dfs)
        file = io.BytesIO(result.to_csv(index=False, sep=';').encode('utf-8'))
        if post_data_to_custom_data_connector(output_filename, file):
            logger.info(f"Successfully updated {output_filename}")
            return True
        else:
            logger.error(f"Failed to update {output_filename}")
            return False
    return False


def create_merge_lambda(merge_func, config):
    if all(key in config for key in ['merge_on', 'group_by', 'agg_columns', 'columns']):
        def merge_lambda(*dfs):
            return merge_func(*dfs, config['merge_on'], config['group_by'], config['agg_columns'], config['columns'])
    else:
        def merge_lambda(*dfs):
            return merge_func(*dfs)
    return merge_lambda


def sags_aktiviteter_merge_df(sager_df, sags_aktivitet_df, borger_df):
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


def sensum_data_merge_df(sager_df, indsatser_df, borger_df):
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


def merge_df_sensum_mål(mål_df, delmål_df, borger_information_df):
    merged_df = pd.merge(mål_df, delmål_df, on='MålId', how='inner', suffixes=('_mål', '_delmål'))
    merged_df = pd.merge(merged_df, borger_information_df, on='BorgerId', how='inner')

    result = merged_df.groupby('MålId').agg({
        "BorgerId": "nunique",
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

    result.columns = ['Counter', 'MålNavn', 'Ansvarlig', 'StartDato', 'SlutDato', 'OprettetDato', 'OprettetAf', 'Lukket',
                      'SamarbejdeMedBorger', 'DelmålNavn', 'Samarbejdspartner', 'EvalueringsDato', 'Fornavn', 'Efternavn', 'CPR', 'Afdeling']

    if pd.isna(result.at[0, 'SlutDato']):
        result.at[0, 'SlutDato'] = pd.Timestamp('1900-01-01')
    if pd.isna(result.at[0, 'Samarbejdspartner']):
        result.at[0, 'Samarbejdspartner'] = 'Ikke angivet'

    return result


def merge_df_ydelse(ydelse_df, borger_information_df, afdeling_df):
    ydelse_df = pd.merge(ydelse_df, afdeling_df[['AfdelingId', 'Navn']], on='AfdelingId', how='left')
    merged_df = pd.merge(ydelse_df, borger_information_df, on='BorgerId', how='inner')

    result = merged_df.groupby('YdelseId').agg({
        "BorgerId": "nunique",
        'YdelseNavn': 'first',
        'StartDato': 'first',
        'SlutDato': 'first',
        'CPR': 'first',
        'Fornavn': 'first',
        'Efternavn': 'first',
        'Navn': 'first',
        'Afdeling': 'first',
    }).reset_index(drop=True)

    result.columns = ['Counter', 'YdelseNavn', 'StartDato', 'SlutDato', 'CPR', 'Fornavn', 'Efternavn', 'AfdelingNavn', 'Afdeling']

    if pd.isna(result.at[0, 'CPR']):
        result.at[0, 'CPR'] = 000000000.0

    return result
