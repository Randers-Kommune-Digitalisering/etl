import logging
import urllib
import pandas as pd

from datetime import datetime

from sd_delta import delta_client, sd_client
from utils.api_requests import APIClient
from utils.ftps import FTPSClient
from utils.utils import df_to_csv_bytes
from utils.config import CONFIG_LIBRARY_URL, CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_BASE_PATH, EKKO_CONFIG_FILE, EKKO_URL, EKKO_USERNAME, EKKO_PASSWORD

logger = logging.getLogger()


def job():
    logger.info('Getting config files')
    config_library_client = APIClient(base_url=CONFIG_LIBRARY_URL, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)
    ekko_config_path = urllib.parse.urljoin(CONFIG_LIBRARY_BASE_PATH, EKKO_CONFIG_FILE)
    sd_department_ids = config_library_client.make_request(path=ekko_config_path)

    sd_department_ids = ['TESS', 'TBVH']  # TODO: Remove, Temporarily only getting two departments

    if not sd_department_ids:
        logging.error(f"Failed to load config file: {EKKO_CONFIG_FILE}")
        return False

    logger.info('Getting department names')
    all_deparments_df = sd_client.get_all_departments_df('RG')

    departments = all_deparments_df[all_deparments_df['DepartmentIdentifier'].isin(sd_department_ids)][['DepartmentIdentifier', 'DepartmentName']].apply(tuple, axis=1).tolist()

    logger.info('Getting user data')

    user_df = get_user_data_df(departments)

    logger.info('Uploading CSV file')

    if upload_csv(user_df):
        logger.info('Successfully uploaded user data to EKKO')
    else:
        logger.error('Failed to update user data')
        return False

    return True


def get_user_data_df(departments: list[tuple[str, str]], institution_id: str = 'RG'):
    all_ekko_employees = []

    for sd_department in departments:
        sd_id = sd_department[0]
        sd_name = sd_department[1]

        employees = delta_client.get_employees_by_sd_department(sd_department_id=sd_id, sd_department_name=sd_name)
        dep_start_dates = sd_client.get_all_start_dates(institution_id=institution_id, department_id=sd_id)
        start_dates = {}
        for sd in dep_start_dates:
            start_dates[sd['employment_id']] = datetime.strptime(sd['employment_date'], "%Y-%m-%d").strftime("%d-%m-%Y")

        for emp in employees:
            emp['Ansættelsesdato'] = start_dates.get(emp['Personalenr.'], None)
            all_ekko_employees.append(emp)

    df = pd.DataFrame(all_ekko_employees)
    # Remove '+45' from mobile phone numbers
    df['Mobiltelefonnr.'] = df['Mobiltelefonnr.'].apply(lambda x: x[-8:] if len(str(x)) > 8 else x)

    return df


def upload_csv(dataframe: pd.DataFrame):
    csv_file = df_to_csv_bytes(dataframe)
    filename = f"ejendomme-og-drift-brugere-{datetime.now().strftime('%d-%m-%Y')}.csv"

    ftps_client = FTPSClient(EKKO_URL, EKKO_USERNAME, EKKO_PASSWORD)

    return ftps_client.put(filename, csv_file)
