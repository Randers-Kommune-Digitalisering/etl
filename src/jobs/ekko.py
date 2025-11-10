import logging
import urllib
import pandas as pd

from datetime import datetime

from sd_client import SD_CLIENT as sd_client
from utils.api_requests import APIClient
from utils.ftps import FTPSClient
from utils.utils import df_to_csv_bytes, check_if_mobile_number_and_clean
from utils.config import CONFIG_LIBRARY_URL, CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_BASE_PATH, EKKO_CONFIG_FILE, EKKO_URL, EKKO_USERNAME, EKKO_PASSWORD

logger = logging.getLogger()


def job():
    logger.info('Getting config files')
    config_library_client = APIClient(base_url=CONFIG_LIBRARY_URL, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)
    ekko_config_path = urllib.parse.urljoin(CONFIG_LIBRARY_BASE_PATH, EKKO_CONFIG_FILE)
    sd_department_ids = config_library_client.make_request(path=ekko_config_path)

    if not sd_department_ids:
        logging.error(f"Failed to load config file: {EKKO_CONFIG_FILE}")
        return False

    logger.info('Getting department names')
    all_deparments_df = sd_client.get_all_departments_df('RG')

    departments = all_deparments_df[all_deparments_df['DepartmentIdentifier'].isin(sd_department_ids)][['DepartmentIdentifier', 'DepartmentName']].apply(tuple, axis=1).tolist()

    logger.info('Getting user data')

    user_df = get_user_data_df(departments)

    user_df.to_csv('user_data.csv', index=False, sep=';')

    logger.info('Uploading CSV file')

    if upload_csv(user_df):
        logger.info('Successfully uploaded user data to EKKO')
    else:
        logger.error('Failed to update user data')
        return False

    return True


def get_user_data_df(departments: list[tuple[str, str]], institution_id: str = 'RG'):
    ekko_employees_df = pd.DataFrame(columns=['Navn', 'Personalenr.', 'Email', 'MasterGroup', 'UserGroup', 'Titel', 'Fødselsdag', 'Ansættelsesdato', 'Mobiltelefonnr.'])

    for sd_department in departments:
        sd_id = sd_department[0]
        sd_name = sd_department[1]

        employees = sd_client.get_employments_by_department(institution_id=institution_id, department_id=sd_id)
        persons = sd_client.get_persons_by_department(institution_id=institution_id, department_id=sd_id)

        for emp in employees:
            person_phones = next((p for p in persons if emp['cpr'] in p['cpr']), None)

            name = next((p['name'] for p in persons if emp['cpr'] in p['cpr']), None)
            employment_id = emp['employment_id']
            email = next((p['email'] for p in persons if emp['cpr'] in p['cpr']), None)
            master_group = 'Ejendomme og Drift'
            user_group = sd_name
            profession = emp['profession']
            birth_day = get_birth_date_from_cpr(emp['cpr'])
            employment_date = emp['employment_date']
            mobile_phone = get_mobile_number(person_phones)

            ekko_employees_df.loc[len(ekko_employees_df)] = [name, employment_id, email, master_group, user_group, profession, birth_day, employment_date, mobile_phone]

    return ekko_employees_df


def get_mobile_number(person_phones: dict):
    mobile_number = None
    for num in person_phones['employment_phones']:
        mobile_number = check_if_mobile_number_and_clean(num)
        if mobile_number:
            break
    if not mobile_number:
        for num in person_phones['person_phones']:
            mobile_number = check_if_mobile_number_and_clean(num)
            if mobile_number:
                break
    return mobile_number


def get_birth_date_from_cpr(cpr_number: str):
    dt = datetime.strptime(cpr_number[:6], "%d%m%y")
    year = dt.year
    today = datetime.today()
    age = today.year - year - ((today.month, today.day) < (dt.month, dt.day))
    if year >= 2000 and age < 18:
        dt = dt.replace(year=year - 100)
    return dt.strftime("%d-%m-%Y")


def upload_csv(dataframe: pd.DataFrame):
    csv_file = df_to_csv_bytes(dataframe)
    filename = f"ejendomme-og-drift-brugere-{datetime.now().strftime('%d-%m-%Y')}.csv"

    ftps_client = FTPSClient(EKKO_URL, EKKO_USERNAME, EKKO_PASSWORD)

    return ftps_client.put(filename, csv_file)
