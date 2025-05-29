import io
import logging
import re
import pandas as pd

from utils.api_requests import APIClient
from sd_client import SDClient
from delta_client import DeltaClient
from logiva_signflow import LogivaSignflowClient
from utils.config import SD_URL, SD_USER, SD_PASS, LOGIVA_URL, LOGIVA_USER, LOGIVA_PASS, MAIL_SERVER_URL, SD_DELTA_FROM_MAIL, SD_DELTA_TO_MAIL, \
    DELTA_URL, DELTA_CLIENT_ID, DELTA_CLIENT_SECRET, DELTA_REALM, DELTA_AUTH_URL
from datetime import datetime

EMPLOYMENT_STATUS = {'0': 'Ansat ikke i løn', '1': 'Aktiv', '3': 'Midlertidig ude af løn', '4': 'Ansat i konflikt', '7': 'Emigreret eller død', '8': 'Fratrådt', '9': 'Pensioneret', 'S': 'Slettet', None: None}

logger = logging.getLogger(__name__)
sd_client = SDClient(SD_URL, SD_USER, SD_PASS)
delta_client = DeltaClient(base_url=DELTA_URL, auth_url=DELTA_AUTH_URL, realm=DELTA_REALM, client_id=DELTA_CLIENT_ID, client_secret=DELTA_CLIENT_SECRET)
ls_client = LogivaSignflowClient(LOGIVA_URL, LOGIVA_USER, LOGIVA_PASS)
mail_client = APIClient(base_url=MAIL_SERVER_URL)


def send_mail_with_attachment(file_name, file_bytes, start_time, end_time):
    payload = {
        'from': SD_DELTA_FROM_MAIL,
        'to': SD_DELTA_TO_MAIL,
        'title': 'SD Delta Robot opdatering',
        'body': f'Vedhæftet er en liste over personer med ændringer i SD og har "nyansat" i Logiva/Signflow for perioden {start_time.strftime("%H:%M:%S %d/%m-%Y")} - {end_time.strftime("%H:%M:%S %d/%m-%Y")}',
        'attachments': {'filename': file_name, 'content': list(file_bytes.getvalue())}
    }

    mail_client.make_request(method='POST', json=payload)


def send_mail():
    payload = {
        'from': SD_DELTA_FROM_MAIL,
        'to': SD_DELTA_TO_MAIL,
        'title': 'SD Delta Robot opdatering',
        'body': 'Ingen brugeroprettelser i SD Delta roboten.'
    }

    mail_client.make_request(method='POST', json=payload)


def df_to_excel_bytes(df):
    excel_file = io.BytesIO()

    with pd.ExcelWriter(excel_file, engine='xlsxwriter') as writer:
        df.to_excel(writer, index=False)

    excel_file.seek(0)

    return excel_file


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
