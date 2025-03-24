import io
import logging
import re
import pytz
import urllib.parse
import pandas as pd

from io import StringIO
from datetime import datetime, timedelta

from utils.api_requests import APIClient
from sd_client import SDClient
from delta_client import DeltaClient
from logiva_signflow import LogivaSignflowClient
from utils.config import SD_URL, SD_USER, SD_PASS, LOGIVA_URL, LOGIVA_USER, LOGIVA_PASS, CONFIG_LIBRARY_URL, CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_BASE_PATH, SD_DELTA_EXCLUDED_DEPARTMENTS_CONFIG_FILE, DELTA_URL, DELTA_CERT_BASE64, DELTA_CERT_PASS

EMPLOYMENT_STATUS = {'0': 'Ansat ikke i løn', '1': 'Aktiv', '3': 'Midlertidig ude af løn', '4': 'Ansat i konflikt', '7': 'Emigreret eller død', '8': 'Fratrådt', '9': 'Pensioneret', 'S': 'Slettet', None: None}

logger = logging.getLogger(__name__)
config_library_client = APIClient(base_url=CONFIG_LIBRARY_URL, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)
sd_client = SDClient(SD_URL, SD_USER, SD_PASS)
ls_client = LogivaSignflowClient(LOGIVA_URL, LOGIVA_USER, LOGIVA_PASS)
delta_client = DeltaClient(DELTA_URL, DELTA_CERT_PASS, DELTA_CERT_BASE64)


def job():
    try:
        excluded_config_path = urllib.parse.urljoin(CONFIG_LIBRARY_BASE_PATH, SD_DELTA_EXCLUDED_DEPARTMENTS_CONFIG_FILE)
        excluded_config_file = config_library_client.make_request(path=excluded_config_path)
        if not excluded_config_file:
            logging.error(f"Failed to load config file: {SD_DELTA_EXCLUDED_DEPARTMENTS_CONFIG_FILE}")
            return False

        excluded_institutions_df = pd.read_csv(StringIO(excluded_config_file.decode("utf-8")), sep=';', skipinitialspace=True).map(lambda x: x.strip() if isinstance(x, str) else x).query('DepartmentIdentifier == "all"')
        excluded_departments_df = pd.read_csv(StringIO(excluded_config_file.decode("utf-8")), sep=';', skipinitialspace=True).map(lambda x: x.strip() if isinstance(x, str) else x).query('DepartmentIdentifier != "all"')

        file_time = datetime.now(tz=pytz.timezone("Europe/Copenhagen"))

        excel_file = get_employments_generate_excel_file(excluded_institutions_df, excluded_departments_df)

        file_name = f'sd-delta-robot_{file_time.strftime("%Y-%m-%d_%H-%M-%S")}.xlsx'

        if excel_file:
            if delta_client.upload_sd_file(file_name, excel_file.read()):
                return True
        return False
    except Exception as e:
        logger.error(e)
        return False


def handle_deleted_employment(employee):
    try:
        in_sd = sd_client.person_exist(employee['institution'], employee['cpr'])
        if in_sd is None:
            raise Exception('Failed to check if person exists in SD')
        elif in_sd:
            logger.info(f'Person with employment {employee["employment_id"]} exists in SD - not making any changes in Delta')
        else:
            can_deactivate, uuid = delta_client.person_can_deactivate(employee['cpr'])
            if can_deactivate:
                if delta_client.person_deactivate(uuid):
                    logger.info(f'Person with employment {employee["employment_id"]} deactivated in Delta')
            else:
                logger.info(f'Person with employment {employee["employment_id"]} has related objects in Delta - not making any changes')
    except Exception as e:
        logger.warning(f'Failed to deactivated person with employment {employee["employment_id"]} with error: {e}')


def get_employments_generate_excel_file(excluded_institutions_df, excluded_departments_df):
    try:
        all_institutions_df = sd_client.get_all_institutions_df()

        if isinstance(all_institutions_df, pd.DataFrame) and not all_institutions_df.empty:
            merged_institutions = all_institutions_df.merge(excluded_institutions_df[["InstitutionIdentifier", "InstitutionName"]], on=['InstitutionIdentifier', 'InstitutionName'], how='left', indicator=True)

            # institutions_to_check is a list of tuples with the institution identifier and name
            institutions_to_check = list(merged_institutions[merged_institutions['_merge'] == 'left_only'].drop(columns=['_merge']).reset_index(drop=True)[["InstitutionIdentifier", "InstitutionName"]].itertuples(index=False, name=None))

            # Signflow autherizations
            signflow_df = ls_client.get_it_department_authorizations_df()
            filtered_signflow_df = signflow_df.loc[(signflow_df['Action'] == 'Nyansat') & (signflow_df['Assigned Login'].isnull())]

            employees_all_instistutions_df = pd.DataFrame(columns=[
                'Institutions-niveau',
                'Stamafdeling',
                'CPR-nummer',
                'Navn (for-/efternavn)',
                'Stillingskode nuværende',
                'Stillingskode niveau 2',
                'Startdato',
                'Slutdato',
                'Ansættelsesstatus',
                'Tjenestenummer',
                'Afdeling',
                'Handling'
            ])

            for inst in institutions_to_check:
                employees = sd_client.get_employments_with_changes(inst[0], datetime.now() - timedelta(days=1), datetime.now())

                if employees:
                    # Get departments to exclude for this institution
                    excluded_departments = excluded_departments_df.loc[excluded_departments_df['InstitutionIdentifier'] == inst[0]]

                    changes_found = len(employees)

                    for employee in employees:

                        if employee['employement_status_code'] == 'S':
                            handle_deleted_employment(employee)
                            continue

                        include_status = False if employee['employement_status_code'] else True

                        extra_employee_details = sd_client.get_employment_details(inst[0], employee['cpr'], employee['employment_id'], employee['effective_date'], include_status=include_status)
                        if extra_employee_details:
                            employee = employee | extra_employee_details
                            department_name = sd_client.get_department_name(inst[0], employee['department'])
                            stripped_department_name = re.sub(r'\(.*\)', '', department_name).strip()

                            if excluded_departments.loc[(excluded_departments['DepartmentIdentifier'] == employee['department']) & (excluded_departments['DepartmentName'] == stripped_department_name)].empty:
                                niveau0, niveau2 = sd_client.get_profession_names(employee['job_position'])
                                employment_status = EMPLOYMENT_STATUS.get(employee['employement_status_code'])
                                if employment_status:
                                    employees_all_instistutions_df = pd.concat([pd.DataFrame([
                                        [
                                            f'{inst[0]} ({inst[1]})',
                                            department_name,
                                            employee['cpr'],
                                            sd_client.get_person_names(inst[0], employee['cpr']),
                                            niveau0,
                                            niveau2,
                                            ".".join(reversed(employee['start_date'].split("-"))),
                                            ".".join(reversed(employee['end_date'].split("-"))),
                                            employment_status,
                                            employee['employment_id'],
                                            employee['department'],
                                            'x' if int(employee['cpr']) in filtered_signflow_df['CPR'].values else None
                                        ]
                                    ], columns=employees_all_instistutions_df.columns), employees_all_instistutions_df], ignore_index=True)
                                else:
                                    logger.warning(f'Employee {employee["employment_id"]} has an unknown status code')
                            else:
                                changes_found -= 1
                        else:
                            changes_found -= 1
                            logger.warning(f'Failed to get extra employee details for employee {employee["employment_id"]} in institution {inst[0]}')

                    logger.info(f'{changes_found} changes found for institution {inst}')

            excel_file = io.BytesIO()

            with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
                employees_all_instistutions_df.to_excel(writer, index=False)

            excel_file.seek(0)

            return excel_file
        else:
            logger.error('Failed to get institutions from SD')
            return
    except Exception as e:
        logger.error(e)
        return
