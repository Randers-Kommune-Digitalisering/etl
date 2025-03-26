import logging
import pytz
import urllib.parse
import pandas as pd

from io import StringIO
from datetime import datetime, timedelta, time

from sd_delta import delta_client, get_employments_with_changes_df, send_mail_with_attachment, df_to_excel_bytes
from utils.api_requests import APIClient

from utils.config import CONFIG_LIBRARY_URL, CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_BASE_PATH, SD_DELTA_EXCLUDED_DEPARTMENTS_CONFIG_FILE


logger = logging.getLogger(__name__)
config_library_client = APIClient(base_url=CONFIG_LIBRARY_URL, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)


def job():
    try:
        excluded_config_path = urllib.parse.urljoin(CONFIG_LIBRARY_BASE_PATH, SD_DELTA_EXCLUDED_DEPARTMENTS_CONFIG_FILE)
        excluded_config_file = config_library_client.make_request(path=excluded_config_path)
        if not excluded_config_file:
            logging.error(f"Failed to load config file: {SD_DELTA_EXCLUDED_DEPARTMENTS_CONFIG_FILE}")
            return False

        excluded_institutions_df = pd.read_csv(StringIO(excluded_config_file.decode("utf-8")), sep=';', skipinitialspace=True).map(lambda x: x.strip() if isinstance(x, str) else x).query('DepartmentIdentifier == "all"')
        excluded_departments_df = pd.read_csv(StringIO(excluded_config_file.decode("utf-8")), sep=';', skipinitialspace=True).map(lambda x: x.strip() if isinstance(x, str) else x).query('DepartmentIdentifier != "all"')

        end_time = datetime.now(pytz.timezone("Europe/Copenhagen"))
        start_time = end_time - timedelta(days=1)

        all_df = get_employments_with_changes_df(excluded_institutions_df, excluded_departments_df, start_time, end_time)

        excel_file = df_to_excel_bytes(all_df)

        file_name = f'sd-delta-robot_{end_time.strftime("%Y-%m-%d_%H-%M-%S")}.xlsx'

        if excel_file:
            if delta_client.upload_sd_file(file_name, excel_file.read()):
                if time(15, 0) <= end_time < time(23, 59, 59):
                    action_only_df = all_df[all_df['Action'] == 'x']
                    action_only_excel_file = df_to_excel_bytes(action_only_df)
                    send_mail_with_attachment(file_name, action_only_excel_file, start_time, end_time)
                return True
        return False
    except Exception as e:
        logger.error(e)
        return False
