import logging
import re
import urllib.parse
import pandas as pd

from io import StringIO
from datetime import datetime

from sd_client import SDClient
from delta_client import DeltaClient
from logiva_signflow import LogivaSignflowClient
from utils.utils import df_to_excel_bytes
from utils.api_requests import APIClient
from utils.config import SD_URL, SD_USER, SD_PASS, LOGIVA_URL, LOGIVA_USER, LOGIVA_PASS, DELTA_URL, DELTA_CLIENT_ID, DELTA_CLIENT_SECRET, DELTA_REALM, DELTA_AUTH_URL, CONFIG_LIBRARY_URL, CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_BASE_PATH, SD_DELTA_EXCLUDED_DEPARTMENTS_CONFIG_FILE

EMPLOYMENT_STATUS = {'0': 'Ansat ikke i løn', '1': 'Aktiv', '3': 'Midlertidig ude af løn', '4': 'Ansat i konflikt', '7': 'Emigreret eller død', '8': 'Fratrådt', '9': 'Pensioneret', 'S': 'Slettet', None: None}

logger = logging.getLogger(__name__)
sd_client = SDClient(SD_URL, SD_USER, SD_PASS)
delta_client = DeltaClient(base_url=DELTA_URL, auth_url=DELTA_AUTH_URL, realm=DELTA_REALM, client_id=DELTA_CLIENT_ID, client_secret=DELTA_CLIENT_SECRET)
ls_client = LogivaSignflowClient(LOGIVA_URL, LOGIVA_USER, LOGIVA_PASS)


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


def get_employments_with_changes_df(excluded_institutions_df, excluded_departments_df, start_datetime, end_datetime, include_logiva=False):
    try:
        all_institutions_df = sd_client.get_all_institutions_df()

        if isinstance(all_institutions_df, pd.DataFrame) and not all_institutions_df.empty:
            logger.info('Handling employments with changes in SD')
            merged_institutions = all_institutions_df.merge(excluded_institutions_df[["InstitutionIdentifier", "InstitutionName"]], on=['InstitutionIdentifier', 'InstitutionName'], how='left', indicator=True)

            # institutions_to_check is a list of tuples with the institution identifier and name
            institutions_to_check = list(merged_institutions[merged_institutions['_merge'] == 'left_only'].drop(columns=['_merge']).reset_index(drop=True)[["InstitutionIdentifier", "InstitutionName"]].itertuples(index=False, name=None))

            # Signflow autherizations
            signflow_df = ls_client.get_it_department_authorizations_df()
            if not isinstance(signflow_df, pd.DataFrame):
                raise Exception('Failed to get signflow authorizations')

            filtered_signflow_df = signflow_df.loc[(signflow_df['Action'] == 'Nyansat') & (signflow_df['Assigned Login'].isnull())]

            all_rows = []

            for inst in institutions_to_check:
                employees = sd_client.get_employments_with_changes(inst[0], start_datetime=start_datetime, end_datetime=end_datetime)

                if employees:
                    # Get departments to exclude for this institution
                    excluded_departments = excluded_departments_df.loc[excluded_departments_df['InstitutionIdentifier'] == inst[0]]

                    changes_found = 0

                    for employee in employees:

                        if employee['employement_status_code'] == 'S':
                            handle_deleted_employment(employee)
                            continue

                        has_active = False

                        for date in employee['effective_dates']:
                            extra_employee_details = sd_client.get_employment_details(inst[0], employee['cpr'], employee['employment_id'], date)

                            if extra_employee_details:
                                if not has_active and extra_employee_details['employement_status_code'] in ['0', '1', '3']:
                                    has_active = True

                                if has_active and extra_employee_details['employement_status_code'] not in ['7', '8', '9'] or not has_active:
                                    employee = employee | extra_employee_details
                                    department_name = sd_client.get_department_name(inst[0], employee['department'])
                                    stripped_department_name = re.sub(r'\(.*\)', '', department_name).strip()

                                    if excluded_departments.loc[(excluded_departments['DepartmentIdentifier'] == employee['department']) & (excluded_departments['DepartmentName'] == stripped_department_name)].empty:
                                        niveau0, niveau2 = sd_client.get_profession_names(employee['job_position'])
                                        employment_status = EMPLOYMENT_STATUS.get(employee['employement_status_code'])
                                        employee_name = sd_client.get_person_names(inst[0], employee['cpr'])

                                        old_start_date = None

                                        if datetime.strptime(employee['start_date'], '%Y-%m-%d').date() < datetime.today().date():
                                            old_start_date = delta_client.get_engagement_start_date_based_on_sd_dates(employee['employment_id'], employee['cpr'][:6], employee['start_date'], employee['end_date'])

                                        if employment_status and employee_name:
                                            row = {
                                                'Institutions-niveau': f'{inst[1]} ({inst[0]})',
                                                'Stamafdeling': department_name,
                                                'CPR-nummer': employee['cpr'],
                                                'Navn (for-/efternavn)': employee_name,
                                                'Stillingskode nuværende': niveau0,
                                                'Stillingskode niveau 2': niveau2,
                                                'Startdato': ".".join(reversed(old_start_date.split("-"))) if old_start_date else ".".join(reversed(employee['start_date'].split("-"))),
                                                'Slutdato': ".".join(reversed(employee['end_date'].split("-"))),
                                                'Ansættelsesstatus': employment_status,
                                                'Tjenestenummer': employee['employment_id'],
                                                'Afdeling': employee['department'],
                                                'Handling': 'x' if employee['cpr'] in filtered_signflow_df['CPR'].values and employee['employement_status_code'] in ['0', '1', '3'] else None
                                            }

                                            all_rows.append(row)
                                            changes_found += 1
                                        else:
                                            logger.warning(f'Employee {employee["employment_id"]} has an unknown status code or name')
                            else:
                                logger.warning(f'Failed to get extra employee details for employee {employee["employment_id"]} in institution {inst[0]} at {date}')

                    logger.info(f'{changes_found} changes found for institution {inst}')

            # Handle logiva
            if include_logiva:
                logger.info('Handling Logiva signflow authorizations')
                all_rows_cprs = {row['CPR-nummer'] for row in all_rows}
                missing_signflow = filtered_signflow_df[~filtered_signflow_df['CPR'].isin(all_rows_cprs)][['CPR', 'From Date']]
                missing_cprs_with_dates = set(missing_signflow.itertuples(index=False, name=None))

                logiva_rows = []

                for cpr, from_date in missing_cprs_with_dates:
                    if from_date and '.' in from_date:
                        try:
                            from_date = datetime.strptime(from_date, "%d.%m.%Y").strftime("%Y-%m-%d")
                        except ValueError:
                            try:
                                from_date = datetime.strptime(from_date, "%d.%m.%y").strftime("%Y-%m-%d")
                            except ValueError:
                                logger.warning("from_date format is invalid: %s", from_date)
                                continue
                    logiva_emp_list = delta_client.get_engagement_without_user(cpr, from_date)
                    if logiva_emp_list:
                        for logiva_emp_details in logiva_emp_list:
                            extra_employee_details = sd_client.get_employment_details(
                                logiva_emp_details['institution_code'],
                                logiva_emp_details['cpr'],
                                logiva_emp_details['employment_id'],
                                from_date
                            )
                            if extra_employee_details:
                                department_name = sd_client.get_department_name(logiva_emp_details['institution_code'], extra_employee_details['department'])
                                niveau0, niveau2 = sd_client.get_profession_names(extra_employee_details['job_position'])
                                employment_status = EMPLOYMENT_STATUS.get(extra_employee_details['employement_status_code'])
                                employee_name = sd_client.get_person_names(logiva_emp_details['institution_code'], logiva_emp_details['cpr'])

                                old_start_date = None

                                if datetime.strptime(extra_employee_details['start_date'], '%Y-%m-%d').date() < datetime.today().date():
                                    old_start_date = delta_client.get_engagement_start_date_based_on_sd_dates(logiva_emp_details['employment_id'], logiva_emp_details['cpr'][:6], extra_employee_details['start_date'], extra_employee_details['end_date'])

                                institution_name = next((name for code, name in institutions_to_check if str(code) == str(logiva_emp_details['institution_code'])), None)
                                if institution_name:
                                    if employment_status and employee_name:
                                        row = {
                                            'Institutions-niveau': f'{institution_name} ({logiva_emp_details["institution_code"]})',
                                            'Stamafdeling': department_name,
                                            'CPR-nummer': logiva_emp_details['cpr'],
                                            'Navn (for-/efternavn)': employee_name,
                                            'Stillingskode nuværende': niveau0,
                                            'Stillingskode niveau 2': niveau2,
                                            'Startdato': ".".join(reversed(old_start_date.split("-"))) if old_start_date else ".".join(reversed(extra_employee_details['start_date'].split("-"))),
                                            'Slutdato': ".".join(reversed(extra_employee_details['end_date'].split("-"))),
                                            'Ansættelsesstatus': employment_status,
                                            'Tjenestenummer': logiva_emp_details['employment_id'],
                                            'Afdeling': extra_employee_details['department'],
                                            'Handling': 'x'
                                        }

                                        logiva_rows.append(row)
                                else:
                                    raise Exception(f'Institution {logiva_emp_details["institution_code"]} not found in institutions_to_check')

                all_rows.extend(logiva_rows)
            return pd.DataFrame(all_rows)
        else:
            logger.error('Failed to get institutions from SD')
            return
    except Exception as e:
        import traceback
        tb = traceback.extract_tb(e.__traceback__)
        if tb:
            line_number = tb[-1].lineno
            logger.error(f"Error on line {line_number}: {e}")
        else:
            logger.error(e)
        return


def get_fixed_end_dates_df():
    res = delta_client.get_all_active_engagements()
    df = pd.DataFrame(res)
    df = df[df['delta_end_date'] != 'PLUS_INF']

    status_cols = ['department', 'job_position', 'employement_status_code', 'employment_status_deactivation_date']
    status_dicts = df.apply(
        lambda row: sd_client.get_employment_status_and_end_date(
            row['institution_code'], row['cpr'], row['tjenestenummer']
        ),
        axis=1
    )
    for col in status_cols:
        df[col] = status_dicts.apply(lambda d: d.get(col))

    df['delta_end_date'] = pd.to_datetime(df['delta_end_date'], errors='coerce') - pd.Timedelta(days=1)
    df['delta_end_date'] = df['delta_end_date'].dt.strftime('%Y-%m-%d')
    df = df[df['employment_status_deactivation_date'] != df['delta_end_date']]

    config_library_client = APIClient(base_url=CONFIG_LIBRARY_URL, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)
    excluded_config_path = urllib.parse.urljoin(CONFIG_LIBRARY_BASE_PATH, SD_DELTA_EXCLUDED_DEPARTMENTS_CONFIG_FILE)
    excluded_config_file = config_library_client.make_request(path=excluded_config_path)
    if not excluded_config_file:
        logging.error(f"Failed to load config file: {SD_DELTA_EXCLUDED_DEPARTMENTS_CONFIG_FILE}")

    all_institutions_df = sd_client.get_all_institutions_df()
    excluded_institutions_df = pd.read_csv(StringIO(excluded_config_file.decode("utf-8")), sep=';', skipinitialspace=True).map(lambda x: x.strip() if isinstance(x, str) else x).query('DepartmentIdentifier == "-"')

    merged_institutions = all_institutions_df.merge(excluded_institutions_df[["InstitutionIdentifier", "InstitutionName"]], on=['InstitutionIdentifier', 'InstitutionName'], how='left', indicator=True)
    institutions_to_check = list(merged_institutions[merged_institutions['_merge'] == 'left_only'].drop(columns=['_merge']).reset_index(drop=True)[["InstitutionIdentifier", "InstitutionName"]].itertuples(index=False, name=None))

    df_read = pd.read_csv("TEST3.csv", dtype=str)

    EMPLOYMENT_STATUS = {'0': 'Ansat ikke i løn', '1': 'Aktiv', '3': 'Midlertidig ude af løn', '4': 'Ansat i konflikt', '7': 'Emigreret eller død', '8': 'Fratrådt', '9': 'Pensioneret', 'S': 'Slettet', None: None}

    all_rows = []

    for idx, row in df_read.iterrows():
        department_name = sd_client.get_department_name(row['institution_code'], row['department'])
        niveau0, niveau2 = sd_client.get_profession_names(row['job_position'])
        employment_status = EMPLOYMENT_STATUS.get(row['employement_status_code'])
        employee_name = sd_client.get_person_names(row['institution_code'], row['cpr'])

        institution_name = next((name for code, name in institutions_to_check if str(code) == str(row['institution_code'])), None)

        if institution_name:
            if employment_status and employee_name:
                row = {
                    'Institutions-niveau': f'{institution_name} ({row["institution_code"]})',
                    'Stamafdeling': department_name,
                    'CPR-nummer': row['cpr'],
                    'Navn (for-/efternavn)': employee_name,
                    'Stillingskode nuværende': niveau0,
                    'Stillingskode niveau 2': niveau2,
                    'Startdato': ".".join(reversed(row['delta_start_date'].split("-"))),
                    'Slutdato': ".".join(reversed(row['employment_status_deactivation_date'].split("-"))),
                    'Ansættelsesstatus': employment_status,
                    'Tjenestenummer': row['tjenestenummer'],
                    'Afdeling': row['department'],
                    'Handling': None
                }
                all_rows.append(row)
    all_df = pd.DataFrame(all_rows)

    excel_file = df_to_excel_bytes(all_df)
    if excel_file:
        with open('rettet_ophørsdatoer.xlsx', "wb") as f:
            f.write(excel_file.read())
        logger.info("Excel file saved to disk: rettet_ophørsdatoer.xlsx")
