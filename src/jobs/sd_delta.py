import io
import logging
import re
import pandas as pd

from datetime import datetime, timedelta

from config.config import SD_DELTA_EXCLUDED_DEPARTMENTS_FILE_PATH, SD_URL, SD_USER, SD_PASS, LOGIVA_URL, LOGIVA_USER, LOGIVA_PASS
from sd_client import SDClient
from logiva_signflow import LogivaSignflowClient

EMPLOYMENT_STATUS = {'0': 'Ansat ikke i løn', '1': 'Aktiv', '3': 'Midlertidig ude af løn', '4': 'Ansat i konflikt', '7': 'Fratrådt', '8': 'Fratrådt', '9': 'Fratrådt', 'S': 'Fratrådt', None: 'Ukendt'}
EXCLUDED_INSTITUTIONS_DF = pd.read_csv(SD_DELTA_EXCLUDED_DEPARTMENTS_FILE_PATH, sep=';', encoding='cp1252', skipinitialspace=True).map(lambda x: x.strip() if isinstance(x, str) else x).query('DepartmentIdentifier == "all"')
EXCLUDED_DEPARTMENTS_DF = pd.read_csv(SD_DELTA_EXCLUDED_DEPARTMENTS_FILE_PATH, sep=';', encoding='cp1252', skipinitialspace=True).map(lambda x: x.strip() if isinstance(x, str) else x).query('DepartmentIdentifier != "all"')

logger = logging.getLogger(__name__)
sd_client = SDClient(SD_URL, SD_USER, SD_PASS)
ls_client = LogivaSignflowClient(LOGIVA_URL, LOGIVA_USER, LOGIVA_PASS)


def job():
    excel_file = generate_excel_file()
    if excel_file:
        # TODO: Start Delta process with the excel file
        return True
    return False


def generate_excel_file():
    try:
        all_institutions_df = sd_client.get_all_institutions_df()

        if isinstance(all_institutions_df, pd.DataFrame) and not all_institutions_df.empty:
            merged_institutions = all_institutions_df.merge(EXCLUDED_INSTITUTIONS_DF[["InstitutionIdentifier", "InstitutionName"]], on=['InstitutionIdentifier', 'InstitutionName'], how='left', indicator=True)
            
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
                # Check changes 10 minutes back TODO: potientially hold last run state in DB
                employees = sd_client.get_employments_with_changes(inst[0], datetime.now() - timedelta(minutes=10), datetime.now())

                if employees:
                    # Get departments to exclude for this institution
                    excluded_departments = EXCLUDED_DEPARTMENTS_DF.loc[EXCLUDED_DEPARTMENTS_DF['InstitutionIdentifier'] == inst[0]]

                    changes_found = len(employees)

                    for employee in employees:
                        extra_employee_details = sd_client.get_employment_details(inst[0], employee['cpr'], employee['employment_id'], employee['effective_date'])
                        if extra_employee_details:
                            employee = employee | extra_employee_details
                            department_name = sd_client.get_department_name(inst[0], employee['department'])
                            stripped_department_name = re.sub(r'\(.*\)', '', department_name).strip()
                            
                            if excluded_departments.loc[(excluded_departments['DepartmentIdentifier'] == employee['department']) & (excluded_departments['DepartmentName'] == stripped_department_name)].empty:
                                niveau0, niveau2 = sd_client.get_profession_names(employee['job_position'])
                                employment_status = EMPLOYMENT_STATUS[employee['status_code']]
                                employees_all_instistutions_df = pd.concat([pd.DataFrame([
                                    [
                                        f'{inst[0]} ({inst[1]})',
                                        department_name,
                                        employee['cpr'],
                                        sd_client.get_person_names(inst[0], employee['cpr']),
                                        niveau0,
                                        niveau2,
                                        ".".join(reversed(employee['start_date'].split("-"))) if employment_status != 'Fratrådt' else ".".join(reversed(employee['employment_date'].split("-"))),
                                        ".".join(reversed(employee['end_date'].split("-"))) if employment_status != 'Fratrådt' else ".".join(reversed(employee['start_date'].split("-"))),
                                        employment_status,
                                        employee['employment_id'],
                                        employee['department'],
                                        'x' if int(employee['cpr']) in filtered_signflow_df['CPR'].values else None
                                    ]
                                ], columns=employees_all_instistutions_df.columns), employees_all_instistutions_df], ignore_index=True)
                            else:
                                changes_found -= 1
                        else:
                            changes_found -= 1
                            logger.warn(f'Failed to get extra employee details for employee {employee["employment_id"]} in institution {inst[0]}')

                    logger.info(f'{changes_found} changes found for institution {inst}')

                else:
                    logger.info(f'No changes found for institution {inst}')

            excel_file = io.BytesIO()
            employees_all_instistutions_df.to_excel(excel_file, index=False)
            excel_file.seek(0)

            print(employees_all_instistutions_df.shape[0])

            return excel_file
        else:
            logger.error('Failed to get institutions from SD')
            return
    except Exception as e:
        logger.error(e)
        return
