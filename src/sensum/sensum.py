import os
import fnmatch
import logging
import pandas as pd
from datetime import datetime, timedelta
from utils.config import SENSUM_IT_SFTP_REMOTE_DIR
from utils.sftp_connection import get_sftp_client
from utils.database_connection import get_db_client
from sqlalchemy.exc import OperationalError
import time

logger = logging.getLogger(__name__)

sftp_client = get_sftp_client()

db_client = get_db_client()


def process_sensum(file_patterns: list, directories: list, merge_func, output_filename):
    try:
        logger.info(f'Starting {output_filename}')
        if isinstance(directories, list) and isinstance(file_patterns, list) and directories and file_patterns:
            with sftp_client.get_connection() as conn:
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
                    return process_and_save_files(file_list_list, conn, merge_func, output_filename)
        else:
            raise Exception("Directories or file patterns are missing")
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


def process_and_save_files(file_list_list, conn, merge_func, output_filename):
    dfs = [handle_files(file_list, conn) for file_list in file_list_list]

    if all(df is not None and not df.empty for df in dfs):
        result = merge_func(*dfs)
        try:
            db_client.ensure_database_exists()
            for file_list in file_list_list:
                connection = db_client.get_connection()
                if connection:
                    logger.info(f"Processing file list: {file_list}")

                    result.to_sql(output_filename.split('.')[0], con=connection, if_exists='replace', index=False)
                    logger.info(f"Successfully saved {output_filename} to the database")
                    connection.close()
                else:
                    raise Exception("Failed to get database connection")
            return True
        except OperationalError as e:
            logger.error(f"Operational error while saving {output_filename} to the database: {e}")
            db_client.rollback_transaction()
            time.sleep(5)
            return process_and_save_files(file_list_list, conn, merge_func, output_filename)
        except Exception as e:
            logger.error(f"Failed to save {output_filename} to the database: {e}")
            db_client.rollback_transaction()
            return False
    return False


def create_merge_lambda(config):
    merge_func = globals().get(config['merge_func'])
    if all(key in config for key in ['merge_on', 'group_by', 'agg_columns', 'columns']):
        def merge_lambda(*dfs):
            return merge_func(*dfs, config['merge_on'], config['group_by'], config['agg_columns'], config['columns'])
    elif all(key in config for key in ['group_by', 'agg_columns', 'columns']):
        def merge_lambda(*dfs):
            return merge_func(*dfs, config['group_by'], config['agg_columns'], config['columns'])
    else:
        raise Exception("Missing required keys in config")
    return merge_lambda


def merge_dataframes(df1, df2, merge_on, group_by, agg_dict, columns):
    try:
        merged_df = pd.merge(df1, df2, on=merge_on, how='inner')
        result = merged_df.groupby(group_by).agg(agg_dict).reset_index(drop=True)
        result.columns = columns
        return result
    except Exception as e:
        raise Exception(f"An error occurred: {e}")


def sags_aktiviteter_merge_df(sager_df, sags_aktivitet_df, borger_df, group_by, agg_dict, columns):
    sager_df = sager_df.rename(columns={'SagModel': 'Sager_SagModel'})

    merged_df = pd.merge(sager_df, sags_aktivitet_df, on='SagId', how='inner')
    merged_df = pd.merge(merged_df, borger_df[['BorgerId', 'CPR', 'Fornavn', 'Efternavn']], on='BorgerId', how='left')

    result = merged_df.groupby(group_by).agg(agg_dict).reset_index(drop=True)
    result.columns = columns

    return result


def sensum_data_merge_df(sager_df, indsatser_df, borger_df, group_by, agg_dict, columns):
    merged_df = pd.merge(indsatser_df, sager_df, on='SagId', how='inner')
    merged_df = pd.merge(merged_df, borger_df, on='BorgerId', how='inner')

    result = merged_df.groupby(group_by).agg(agg_dict).reset_index(drop=True)
    result.columns = columns

    return result


def merge_df_sensum_mål_and_delmål(
    mål_indikator_df, mål_df, borger_information_df, indikator_df,
    indikator_katalog_df, indikator_kategori_df, indikator_underkategori_df,
    indikator_svar_df, indikator_variabel_df, indikator_værdisæt_df,
    indikator_værdi_df, delmål_df, group_by,
    agg_dict, columns
):
    indikator_katalog_df = indikator_katalog_df.rename(columns={'Navn': 'IndikatorKatalogNavn', 'Aktiv': 'IndikatorKatalogAktiv'})
    indikator_df = indikator_df.rename(columns={'Navn': 'IndikatorNavn', 'Aktiv': 'IndikatorAktiv'})
    indikator_kategori_df = indikator_kategori_df.rename(columns={'IndikatorKategoriNavn': 'IndikatorKategoriNavn', 'Aktiv': 'IndikatorKategoriAktiv'})
    indikator_underkategori_df = indikator_underkategori_df.rename(columns={'Aktiv': 'IndikatorUnderKategoriAktiv'})
    indikator_variabel_df = indikator_variabel_df.rename(columns={'Navn': 'IndikatorVariabelNavn'})
    indikator_svar_df = indikator_svar_df.rename(columns={'OprettetDato': 'IndikatorSvarOprettetDato'})
    indikator_værdisæt_df = indikator_værdisæt_df.rename(columns={'Navn': 'IndikatorVærdisætNavn'})
    indikator_værdi_df = indikator_værdi_df.rename(columns={'Navn': 'IndikatorVærdiNavn'})
    mål_df = mål_df.rename(columns={'OprettetDato': 'MålDato'})

    merged_df = pd.merge(mål_indikator_df, mål_df, on='MålId', how='inner')
    merged_df = pd.merge(merged_df, indikator_df, on='IndikatorId', how='inner')
    merged_df = pd.merge(merged_df, borger_information_df, on='BorgerId', how='inner')
    merged_df = merged_df.drop(columns=['IndikatorKatalogId_y'])
    merged_df = pd.merge(merged_df, indikator_katalog_df, left_on='IndikatorKatalogId_x', right_on='IndikatorKatalogId', how='inner')
    merged_df = pd.merge(merged_df, indikator_kategori_df, on='IndikatorKategoriId', how='inner')
    merged_df = pd.merge(merged_df, indikator_underkategori_df, on='IndikatorUnderKategoriId', how='inner')
    merged_df = pd.merge(merged_df, indikator_variabel_df, on='IndikatorVariabelId', how='inner')
    merged_df = pd.merge(merged_df, indikator_svar_df[['IndikatorSvarId', 'IndikatorSvarOprettetDato', 'IndikatorVariabelId']], on='IndikatorVariabelId', how='inner')
    merged_df = pd.merge(merged_df, indikator_værdisæt_df, on='IndikatorVærdiSætId', how='inner')
    merged_df = pd.merge(merged_df, indikator_værdi_df, on='IndikatorVærdiSætId', how='inner')
    merged_df = pd.merge(merged_df, delmål_df[['MålId', 'EvalueringsDato', 'DelmålNavn']], on='MålId', how='left')

    result = merged_df.groupby(group_by).agg(agg_dict).reset_index(drop=True)
    result.columns = columns

    return result


def merge_df_ydelse(ydelse_df, borger_information_df, afdeling_df, group_by, agg_dict, columns):
    ydelse_df = pd.merge(ydelse_df, afdeling_df[['AfdelingId', 'Navn']], on='AfdelingId', how='left')
    merged_df = pd.merge(ydelse_df, borger_information_df, on='BorgerId', how='inner')

    result = merged_df.groupby(group_by).agg(agg_dict).reset_index(drop=True)
    result.columns = columns

    return result


def sager_afdeling_medarbejder_merge_df(sager_df, afdeling_df, medarbejder_df, group_by, agg_dict, columns):
    sager_df = sager_df.rename(columns={'SagModel': 'Sager_SagModel'})
    afdeling_df = afdeling_df.rename(columns={'Navn': 'AfdelingNavn', 'AfdelingsId': 'AfdelingId'})
    medarbejder_df = medarbejder_df.rename(columns={'Fornavn': 'MedarbejderFornavn', 'Efternavn': 'MedarbejderEfternavn'})
    merged_df = pd.merge(sager_df, afdeling_df[['AfdelingId', 'AfdelingNavn']], on='AfdelingId', how='left')

    merged_df = merged_df.rename(columns={'AfdelingNavn_y': 'AfdelingNavn'})
    merged_df = pd.merge(merged_df, medarbejder_df[['MedarbejderId', 'MedarbejderFornavn', 'MedarbejderEfternavn', 'AfdelingId']], on='AfdelingId', how='left')

    merged_df = merged_df[merged_df['Status'] == 'Igangværende']
    result = merged_df.groupby(group_by).agg(agg_dict).reset_index(drop=True)
    result.columns = columns
    return result


def indsats_df(indsats_df, group_by, agg_dict, columns):
    indsats_df = indsats_df.rename(columns={'StartDato': 'IndsatsStartDato', 'Id': 'IndsatsId', 'Status': 'IndsatsStatus'})

    result = indsats_df.groupby(group_by).agg(agg_dict).reset_index(drop=True)
    result.columns = columns

    return result
