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

# TODO: Fix and remove comments
# TODO: remove all commented out code


# TODO: Refactor - combine process_sensum and process_sensum_latest into one function
def process_sensum(file_patterns, process_func, merge_func, output_filename):
    try:
        logger.info(f'Starting {output_filename}')
        with sftp_client.get_connection() as conn:
            files_list = [get_files(connection=conn, directory=SENSUM_IT_SFTP_REMOTE_DIR, subdirectory='sensum_randers', pattern=pattern) for pattern in file_patterns]

            if all(files_list):
                return process_and_post_files(files_list, conn, process_func, merge_func, output_filename)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False

    return False


# TODO: Refactor - combine process_sensum and process_sensum_latest into one function
def process_sensum_latest(file_patterns, directories, process_func, merge_func, output_filename):
    try:
        logger.info(f'Starting {output_filename}')
        with sftp_client.get_connection() as conn:
            file_list_list = []
            for pattern in file_patterns:
                files_list = []
                for dir in directories:
                    files_list = files_list + get_files(connection=conn, directory=SENSUM_IT_SFTP_REMOTE_DIR, subdirectory=dir, pattern=pattern, only_latest=True)
                if files_list:
                    file_list_list.append(files_list)
                else:
                    raise Exception(f"No files found for pattern {pattern}")

            if all(file_list_list):
                return process_and_post_files_latest(file_list_list, conn, process_func, merge_func, output_filename)
    except Exception as e:
        logger.error(f"An error occurred: {e}")
        return False


# TODO: Refactor - combine handle_files and files_to_df into one function
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

        df = pd.concat(df_list, ignore_index=True)
        df = df.drop_duplicates()
        return df

    except Exception as e:
        logger.error(f"Error handling files: {e}")
        return None


# TODO: Refactor - combine handle_files and files_to_df into one function
def files_to_df(files, connection):
    try:
        df_list = []
        for file in files:
            with connection.open(file) as f:
                df = pd.read_csv(f, sep=";", header=0, decimal=",")
                df_list.append(df)

        if df_list:
            df = pd.concat(df_list, ignore_index=True)
            df = df.drop_duplicates()
            return df
        else:
            logger.info("No files found.")

    except Exception as e:
        logger.error(f"Error converting files to df: {e}")


# def handle_sub_folder_files(subfolders, connection, file_pattern, base_dir):
#     try:
#         df_list = []
#         for subfolder in subfolders:
#             folder_path = os.path.join(base_dir, subfolder)
#             files = [f for f in connection.listdir(folder_path) if fnmatch.fnmatch(f, file_pattern)]

#             if not files:
#                 logger.info(f"No files found in {subfolder} matching pattern {file_pattern}")
#                 continue

#             for file in files:
#                 logger.info(f"Found file in {subfolder}: {file}")

#             latest_file = max(files, key=lambda f: connection.stat(os.path.join(folder_path, f)).st_mtime)
#             logger.info(f"Latest file in {subfolder}: {latest_file}")

#             with connection.open(os.path.join(folder_path, latest_file).replace("\\", "/")) as f:
#                 df = pd.read_csv(f, sep=";", header=0, decimal=",")
#                 df_list.append(df)

#         if df_list:
#             df = pd.concat(df_list, ignore_index=True)
#             df = df.drop_duplicates()
#             return df
#         else:
#             logger.info("No files found in the specified subfolders.")
#             return None

#     except Exception as e:
#         logger.error(f"Error handling files: {e}")
#         return None


# def get_subfolders():
#     return ['Baa', 'BeVej', 'BoAu', 'Born_Bo', 'Bvh', 'Frem', 'Hjorne', 'Job', 'Lade', 'Lbg', 'Meau', 'Mepu',
#             'P4', 'Phus', 'Psyk', 'Senhj', 'STU']


def merge_dataframes(df1, df2, merge_on, group_by, agg_dict, columns):
    try:
        merged_df = pd.merge(df1, df2, on=merge_on, how='inner')
        result = merged_df.groupby(group_by).agg(agg_dict).reset_index(drop=True)
        result.columns = columns
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


# TODO: Refactor - combine process_and_post_files and process_and_post_files_latest into one function
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


# TODO: Refactor - combine process_and_post_files and process_and_post_files_latest into one function
def process_and_post_files_latest(file_list_list, conn, process_func, merge_func, output_filename):
    dfs = [process_func(file_list, conn) for file_list in file_list_list]

    if all(not df.empty for df in dfs):
        result = merge_func(*dfs)
        file = io.BytesIO(result.to_csv(index=False, sep=';').encode('utf-8'))
        if post_data_to_custom_data_connector(output_filename, file):
            logger.info(f"Successfully updated {output_filename}")
            return True
        else:
            logger.error(f"Failed to update {output_filename}")
