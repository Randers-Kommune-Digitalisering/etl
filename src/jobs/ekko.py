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


def job() -> bool:
    """
    Main job function to update EKKO user data.

    :return: Job success status
    :rtype: bool
    """
    logger.info('Getting config files')
    config_library_client = APIClient(base_url=CONFIG_LIBRARY_URL, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)
    ekko_config_path = urllib.parse.urljoin(CONFIG_LIBRARY_BASE_PATH, EKKO_CONFIG_FILE)
    ekko_config_file = config_library_client.make_request(path=ekko_config_path)

    sd_department_ids = ekko_config_file.get('departments', []) if ekko_config_file else []
    sd_employee_ids = ekko_config_file.get('employees', []) if ekko_config_file else []

    if not sd_department_ids:
        logging.error(f"Failed to load config file: {EKKO_CONFIG_FILE}")
        return False

    logger.info('Getting department names')
    all_deparments_df = sd_client.get_all_departments_df('RG')

    filtered_departments = all_deparments_df[all_deparments_df['DepartmentIdentifier'].isin(sd_department_ids)][['DepartmentIdentifier', 'DepartmentName']].apply(tuple, axis=1).tolist()

    logger.info('Getting user data')

    user_df = _get_user_data_df(departments=filtered_departments, all_deparments_df=all_deparments_df)

    if sd_employee_ids:
        filtered_departments = all_deparments_df[['DepartmentIdentifier', 'DepartmentName']].apply(tuple, axis=1).tolist()
        tmp_user_df = _get_user_data_df_by_employment_ids(employment_ids=sd_employee_ids, departments=filtered_departments, all_deparments_df=all_deparments_df)
        user_df = pd.concat([user_df, tmp_user_df]).drop_duplicates().reset_index(drop=True)

    logger.info('Uploading CSV file')

    if _upload_csv(user_df):
        logger.info('Successfully uploaded user data to EKKO')
    else:
        logger.error('Failed to update user data')
        return False

    return True


def _get_user_data_df(departments: list[tuple[str, str]], institution_id: str = 'RG', all_deparments_df: pd.DataFrame = None) -> pd.DataFrame:
    """
    Get user data DataFrame from SD client.

    :param departments: List of department tuples (id, name)
    :type departments: list[tuple[str, str]]
    :param institution_id: Institution identifier
    :type institution_id: str
    :param all_deparments_df: DataFrame containing all departments
    :type all_deparments_df: pd.DataFrame
    :return: DataFrame containing user data
    :rtype: pd.DataFrame
    """
    ekko_employees_df = pd.DataFrame(columns=['Navn', 'Personalenr.', 'Email', 'MasterGroup', 'UserGroup', 'Titel', 'Fødselsdag', 'Ansættelsesdato', 'Mobiltelefonnr.'])
    org = sd_client.get_all_organization(institution_id)

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

            master_group_id = _find_level3_parent_code(org=org, child_code=sd_id)
            master_group = all_deparments_df.loc[all_deparments_df['DepartmentIdentifier'] == master_group_id, 'DepartmentName'].squeeze() if master_group_id else None

            user_group = sd_name
            profession = emp['profession']
            birth_day = _get_birth_date_from_cpr(emp['cpr'])
            employment_date = emp['employment_date']
            mobile_phone = _get_mobile_number(person_phones)

            ekko_employees_df.loc[len(ekko_employees_df)] = [name, employment_id, email, master_group, user_group, profession, birth_day, employment_date, mobile_phone]

    return ekko_employees_df


def _get_user_data_df_by_employment_ids(employment_ids: list[int], departments: list[tuple[str, str]], all_deparments_df: pd.DataFrame) -> pd.DataFrame:
    """
    Get user data DataFrame from SD client by employment IDs.

    :param employment_ids: List of employment IDs
    :type employment_ids: list[int]
    :param departments: List of department tuples (id, name)
    :type departments: list[tuple[str, str]]
    :param all_deparments_df: DataFrame containing all departments
    :type all_deparments_df: pd.DataFrame
    :return: DataFrame containing user data
    :rtype: DataFrame
    """
    ekko_employees_df = pd.DataFrame(columns=['Navn', 'Personalenr.', 'Email', 'MasterGroup', 'UserGroup', 'Titel', 'Fødselsdag', 'Ansættelsesdato', 'Mobiltelefonnr.'])
    org = sd_client.get_all_organization('RG')
    for emp_id in employment_ids:
        per = sd_client.get_person_by_employment_id(institution_id='RG', employment_id=emp_id)
        emp = sd_client.get_employment_by_employment_id(institution_id='RG', employment_id=emp_id)

        master_group_id = _find_level3_parent_code(org=org, child_code=emp['department_id'])
        master_group = all_deparments_df.loc[all_deparments_df['DepartmentIdentifier'] == master_group_id, 'DepartmentName'].squeeze() if master_group_id else None

        user_group = next((dept_name for dept_id, dept_name in departments if dept_id == emp['department_id']), None)
        profession = emp['profession']
        birth_day = _get_birth_date_from_cpr(emp['cpr'])
        employment_date = emp['employment_date']
        mobile_phone = _get_mobile_number(per)

        name = per['name']
        employment_id = emp_id
        email = per['email']

        ekko_employees_df.loc[len(ekko_employees_df)] = [name, employment_id, email, master_group, user_group, profession, birth_day, employment_date, mobile_phone]
    return ekko_employees_df


def _contains_department(dept: dict, target_code: str) -> bool:
    """Check if a department or its sub-departments contain the target department code."""
    if dept['DepartmentCode'] == target_code:
        return True
    return any(_contains_department(sub, target_code) for sub in dept.get('Departments', []))


def _find_level3_parent_code(org: list[dict], child_code: str) -> str | None:
    """Find the level 3 parent department code for a given child department code."""
    for dept in org:
        if _contains_department(dept, child_code):
            if dept['DepartmentLevel'] == '3':
                return dept['DepartmentCode']
            result = _find_level3_parent_code(dept.get('Departments', []), child_code)
            if result:
                return result
    return None


def _get_mobile_number(person_phones: dict) -> str | None:
    """
    Attempt to retrieve a mobile number from the provided phone data.
    Prioritizes employment phones over personal phones.

    :param person_phones: Dictionary containing employment and personal phone numbers. List under keys 'employment_phones' and 'person_phones'.
    :type person_phones: dict
    :return: Mobile phone number if found, otherwise None
    :rtype: str | None
    """
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


def _get_century_from_cpr(cpr_number: str) -> int:
    """
    Determine the century of birth from a CPR number, using the control digit rules.
    Rules based on Danish CPR number system: https://www.cpr.dk/cpr-systemet/opbygning-af-cpr-nummeret

    :param cpr_number: CPR number string
    :type cpr_number: str
    :return: Century of birth (e.g., 1900, 2000, 1800)
    :rtype: int
    """
    first_control_digit = int(cpr_number[6])
    if first_control_digit in [0, 1, 2, 3]:
        return 1900
    short_year = int(cpr_number[4:6])
    if first_control_digit in [4, 9]:
        if short_year >= 37:
            return 1900
        else:
            return 2000
    elif first_control_digit in [5, 6, 7, 8]:
        if short_year <= 57:
            return 2000
        else:
            return 1800


def _get_birth_date_from_cpr(cpr_number: str) -> str:
    """
    Extract and format the birth date from a CPR number.

    :param cpr_number: CPR number string
    :type cpr_number: str
    :return: Birth date in DD-MM-YYYY format
    :rtype: str
    """
    century = _get_century_from_cpr(cpr_number)
    dt = datetime(year=century + int(cpr_number[4:6]), month=int(cpr_number[2:4]), day=int(cpr_number[0:2]))
    return dt.strftime("%d-%m-%Y")


def _upload_csv(dataframe: pd.DataFrame) -> bool:
    """
    Upload the given DataFrame as a CSV file to the EKKO system via FTPS.

    :param dataframe: DataFrame to be uploaded as CSV
    :type dataframe: pd.DataFrame
    :return: True if upload was successful, False otherwise
    :rtype: bool
    """
    csv_file = df_to_csv_bytes(dataframe)
    filename = f"ejendomme-og-drift-brugere-{datetime.now().strftime('%d-%m-%Y')}.csv"

    ftps_client = FTPSClient(EKKO_URL, EKKO_USERNAME, EKKO_PASSWORD)

    return ftps_client.put(filename, csv_file)
