import logging
import pytz
import urllib.parse
import pandas as pd

from collections import defaultdict
from io import StringIO
from datetime import datetime, timedelta

from sd_delta import delta_client, get_employments_with_changes_df
from utils.utils import df_to_excel_bytes
from utils.api_requests import APIClient

from utils.config import CONFIG_LIBRARY_URL, CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_BASE_PATH, SD_DELTA_EXCLUDED_DEPARTMENTS_CONFIG_FILE


logger = logging.getLogger(__name__)
config_library_client = APIClient(base_url=CONFIG_LIBRARY_URL, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)


def job():
    try:
        logger.info("Starting SD Delta job...")
        excluded_config_path = urllib.parse.urljoin(CONFIG_LIBRARY_BASE_PATH, SD_DELTA_EXCLUDED_DEPARTMENTS_CONFIG_FILE)
        excluded_config_file = config_library_client.make_request(path=excluded_config_path)
        if not excluded_config_file:
            logging.error(f"Failed to load config file: {SD_DELTA_EXCLUDED_DEPARTMENTS_CONFIG_FILE}")
            return False

        excluded_institutions_df = pd.read_csv(StringIO(excluded_config_file.decode("utf-8")), sep=';', skipinitialspace=True).map(lambda x: x.strip() if isinstance(x, str) else x).query('DepartmentIdentifier == "-"')
        excluded_departments_df = pd.read_csv(StringIO(excluded_config_file.decode("utf-8")), sep=';', skipinitialspace=True).map(lambda x: x.strip() if isinstance(x, str) else x).query('DepartmentIdentifier != "-"')

        end_time = datetime.now(pytz.timezone("Europe/Copenhagen"))
        start_time = end_time - timedelta(days=2)

        include_logiva = True

        all_df = get_employments_with_changes_df(excluded_institutions_df, excluded_departments_df, start_time, end_time, include_logiva)

        # Split all_df into the minimum number of DataFrames so that no 'CPR-nummer' is repeated within a DataFrame
        def split_df_no_duplicate_cpr(df, column):

            # Count occurrences of each CPR-nummer
            value_counts = df[column].value_counts()
            max_count = value_counts.max()

            # Create empty DataFrames for each split
            dfs = [pd.DataFrame(columns=df.columns) for _ in range(max_count)]

            # Track how many times each CPR-nummer has been assigned
            cpr_assign_count = defaultdict(int)

            for idx, row in df.iterrows():
                cpr = row[column]
                assign_idx = cpr_assign_count[cpr]
                dfs[assign_idx] = pd.concat([dfs[assign_idx], pd.DataFrame([row])], ignore_index=True)
                cpr_assign_count[cpr] += 1

            # Remove empty DataFrames (in case some are unused)
            dfs = [d for d in dfs if not d.empty]
            return dfs

        dfs_no_duplicates = split_df_no_duplicate_cpr(all_df, "CPR-nummer")

        for i, df in enumerate(dfs_no_duplicates):
            excel_file = df_to_excel_bytes(df)

            file_name = f'{i + 1}_sd-delta-robot_{end_time.strftime("%Y-%m-%d_%H-%M-%S")}.xlsx'

            if excel_file:
                if delta_client.upload_sd_file(file_name, excel_file.read()):
                    logger.info(f"Successfully uploaded file: {file_name} ({i + 1}/{len(dfs_no_duplicates)})")
            else:
                logger.error(f"Failed to create Excel file for DataFrame {i + 1}/{len(dfs_no_duplicates)}")
        return True
    except Exception as e:
        logger.error(e)
        return False
