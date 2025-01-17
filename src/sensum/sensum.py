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


def get_sensum(file_patterns, process_func, merge_func, output_filename):
    try:
        logger.info(f'Starting {output_filename}')
        sftp_client = SFTPClient(SENSUM_IT_SFTP_HOST, SENSUM_IT_SFTP_USER, password=SENSUM_IT_SFTP_PASS)
        conn = sftp_client.get_connection()
        files_list = [get_files(conn, pattern) for pattern in file_patterns]

        if all(files_list):
            return process_and_post_files(files_list, conn, process_func, merge_func, output_filename)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False

    return False


def handle_files(files, connection):
    try:
        latest_file = max(files, key=lambda f: connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, f)).st_mtime)
        logger.info(f"Latest file: {latest_file}")

        last_modified_time = connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, latest_file)).st_mtime
        date = datetime.fromtimestamp(last_modified_time)

        max_date = date - timedelta(days=1)
        min_date = datetime(date.year - 2, date.month, 1)

        logger.info(f'Data period: {min_date} - {max_date}')

        files = [f for f in files if datetime.fromtimestamp(connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, f)).st_mtime) >= min_date]

        df_list = []
        min_date = pd.to_datetime(min_date)

        for filename in files:
            with connection.open(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, filename).replace("\\", "/")) as f:
                df = pd.read_csv(f, sep=";", header=0, decimal=",")
                df_list.append(df)

        return pd.concat(df_list, ignore_index=True)
    except Exception as e:
        logger.error(f"Error handling files: {e}")
        return None


def merge_dataframes(df1, df2, merge_on, group_by, agg_dict, columns):
    try:
        merged_df = pd.merge(df1, df2, on=merge_on, how='inner')
        result = merged_df.groupby(group_by).agg(agg_dict).reset_index(drop=True)
        result.columns = columns
    except Exception as e:
        raise Exception(f"An error occurred: {e}")
    return result


def get_files(conn, pattern):
    try:
        files = [f for f in conn.listdir(SENSUM_IT_SFTP_REMOTE_DIR) if fnmatch.fnmatch(f, pattern)]
    except Exception as e:
        raise Exception(f"An error occurred while getting files: {e}")
    return files


def process_and_post_files(files_list, conn, process_func, merge_func, output_filename):
    dfs = [process_func(files, conn) for files in files_list]

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
