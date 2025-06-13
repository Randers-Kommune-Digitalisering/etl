import logging
import pytz
import pandas as pd

from lxml import etree
from datetime import datetime, timedelta

from utils.api_requests import APIClient
from utils.utils import flatten_xml

logger = logging.getLogger(__name__)


class SDClient(APIClient):
    def __init__(self, base_url, username, password):
        super().__init__(base_url, username=username, password=password)

    # Returns a pandas dataframe
    def get_all_institutions_df(self):
        try:
            params = {'RegionIdentifier': '9R'}

            res = self.make_request(method='POST', path='GetInstitution20080201', params=params)

            return pd.read_xml(res, xpath='.//Institution')
        except Exception as e:
            logger.error(e)
            return None

    # Returns a pandas dataframe
    def get_all_departments_df(self, institution_id, activation_date=datetime.now(), deactivation_date=datetime.now()):
        try:
            params = {
                'InstitutionIdentifier': institution_id,
                'ActivationDate': activation_date.strftime("%Y-%m-%d"),
                'DeactivationDate': deactivation_date.strftime("%Y-%m-%d"),
                'DepartmentNameIndicator': True
            }

            res = self.make_request(method='POST', path='GetDepartment20111201', params=params)

            return pd.read_xml(res, xpath='.//Department')
        except Exception as e:
            logger.error(e)
            return None

    # Returns a string with the department name and the institution and department id
    # in the form of: DepartmentName (InstitutionCode_DepartmentCode)
    def get_department_name(self, institution_id, department_id, start_datetime=datetime.now(), end_datetime=datetime.now()):
        try:
            params = {
                'InstitutionIdentifier': institution_id,
                'DepartmentIdentifier': department_id,
                'ActivationDate': start_datetime.strftime("%Y-%m-%d"),
                'DeactivationDate': end_datetime.strftime("%Y-%m-%d"),
                'DepartmentNameIndicator': True,
            }

            res = self.make_request(method='POST', path='GetDepartment20080201', params=params)

            root = etree.fromstring(res)

            departments = root.xpath("//Department")

            if len(departments) == 1:
                flattened_data = flatten_xml(departments[0])
                return f'{flattened_data["DepartmentName"]} ({institution_id}_{flattened_data["DepartmentIdentifier"]})'
            else:
                raise Exception('No or multiple results found')
        except Exception as e:
            logger.error(e)
            return None

    # Returns a tuple with the profession names and codes for niveau 0 and niveau 2
    # In the form of: (ProfessionName0 (InstitutionCode_ProfessionCode), ProfessionName2 (InstitutionCode_ProfessionCode))
    def get_profession_names(self, profession_id):
        try:
            # Always RG for professions, even if the profession is from another institution
            params = {'InstitutionIdentifier': 'RG', 'JobPositionIdentifier': profession_id}

            res = self.make_request(method='POST', path='GetProfession20080201', params=params)

            df = pd.read_xml(res, xpath='.//Profession')

            if all(x in df['JobPositionLevelCode'].unique() for x in [0, 2]):
                return (f"{df.loc[df['JobPositionLevelCode'] == 0, 'JobPositionName'].iloc[0]} ({'RG'}_{profession_id})",
                        f"{df.loc[df['JobPositionLevelCode'] == 2, 'JobPositionName'].iloc[0]} ({'RG'}_{df.loc[df['JobPositionLevelCode'] == 2, 'JobPositionIdentifier'].iloc[0].astype(str).zfill(4)})")
            elif 0 in df['JobPositionLevelCode'].unique():
                logger.warning(f'niveau 2 not found for profession {profession_id} - setting it to "Basispersonale (RG_0005)"')
                return (f"{df.loc[df['JobPositionLevelCode'] == 0, 'JobPositionName'].iloc[0]} ({'RG'}_{profession_id})", 'Basispersonale (RG_0005)')
            else:
                raise Exception(f'niveau 0 not found for profession {profession_id}')

        except Exception as e:
            logger.error(e)
            return (None, None)

    # Returns a string with the person full name
    # In the form of: PersonGivenName PersonSurnameName
    def get_person_names(self, institution_id, cpr_id, effective_date=datetime.now()):
        try:
            params = {
                'InstitutionIdentifier': institution_id,
                'PersonCivilRegistrationIdentifier': cpr_id,
                'EffectiveDate': effective_date.strftime("%Y-%m-%d"),
                'StatusActiveIndicator': True,
                'StatusPassiveIndicator': True
            }

            res = self.make_request(method='POST', path='GetPerson', params=params)

            root = etree.fromstring(res)

            persons = root.xpath("//Person")

            if len(persons) == 1:
                flattened_data = flatten_xml(persons[0])
                return f'{flattened_data["PersonGivenName"]} {flattened_data["PersonSurnameName"]}'
            else:
                raise Exception('No or multiple results found')
        except Exception as e:
            logger.error(e)
            return None

    def person_exist(self, institution_id, cpr_id, effective_date=datetime.now()):
        try:
            params = {
                'InstitutionIdentifier': institution_id,
                'PersonCivilRegistrationIdentifier': cpr_id,
                'EffectiveDate': effective_date.strftime("%Y-%m-%d"),
                'StatusActiveIndicator': True,
                'StatusPassiveIndicator': True
            }

            res = self.make_request(method='POST', path='GetPerson', params=params)

            res_string = res.decode()

            if '<Fault>' in res_string:
                if 'PersonCivilRegistrationIdentifier' in res_string:
                    return False
                else:
                    raise Exception('api error')
            else:
                return True

        except Exception as e:
            logger.error(e)

    # Returns a tuple with the department id and the profession id
    def get_employment_details(self, institution_id, cpr_id, employee_id, effective_date=None):
        try:
            effective_date = datetime.now(pytz.timezone("Europe/Copenhagen")).strftime("%Y-%m-%d") if effective_date is None else effective_date

            params = {
                'InstitutionIdentifier': institution_id,
                'EmploymentIdentifier': employee_id,
                'PersonCivilRegistrationIdentifier': cpr_id,
                'EffectiveDate': effective_date,
                'StatusActiveIndicator': True,
                'StatusPassiveIndicator': True,
                'ProfessionIndicator': True,
                'DepartmentIndicator': True,
                'SalaryCodeGroupIndicator': False,
                'WorkingTimeIndicator': False,
                'EmploymentStatusIndicator': True,
                'SalaryAgreementIndicator': False
            }

            res = self.make_request(method='POST', path='GetEmployment20070401', params=params)

            root = etree.fromstring(res)

            persons = root.xpath("//Person")

            if len(persons) == 1:
                p = persons[0]
                employment = {}

                # This is not used - but keeping it for now
                # employment_date = p.find('Employment/EmploymentDate')
                # employment['employment_date'] = employment_date.text if employment_date is not None else None

                department = p.find('Employment/Department/DepartmentIdentifier')
                employment['department'] = department.text if department is not None else None

                job_position = p.find('Employment/Profession/JobPositionIdentifier')
                employment['job_position'] = job_position.text if job_position is not None else None

                status_code = p.find('Employment/EmploymentStatus/EmploymentStatusCode')
                employment['employement_status_code'] = status_code.text if status_code is not None else None

                start_dates = [date.strftime('%Y-%m-%d') for date in sorted([datetime.strptime(date, '%Y-%m-%d') for date in list(set(p.xpath('.//ActivationDate/text()')))])]
                employment['start_date'] = start_dates[-1]

                end_dates = [date.strftime('%Y-%m-%d') for date in sorted([datetime.strptime(date, '%Y-%m-%d') for date in list(set(p.xpath('.//DeactivationDate/text()')))])]
                employment['end_date'] = end_dates[0]

                return employment
            elif len(persons) == 0:
                # raise Exception('No results found')
                logger.warning('No results found')
            else:
                raise Exception('Multiple results found')
        except Exception as e:
            logger.error(e)
            return None

    def get_employments_with_changes(self, institution_id, start_datetime=datetime.now() - timedelta(hours=1), end_datetime=datetime.now()):
        try:
            start_date = start_datetime.strftime("%Y-%m-%d")
            start_time = start_datetime.strftime("%H:%M:%S")
            end_date = end_datetime.strftime("%Y-%m-%d")
            end_time = end_datetime.strftime("%H:%M:%S")

            params = {
                'InstitutionIdentifier': institution_id,
                'ActivationDate': start_date,
                'ActivationTime': start_time,
                'DeactivationDate': end_date,
                'DeactivationTime': end_time,
                'EmploymentStatusIndicator': True,
                'ProfessionIndicator': True,
                'DepartmentIndicator': True,
                'SalaryCodeGroupIndicator': False,
                'WorkingTimeIndicator': False,
                'SalaryAgreementIndicator': False,
                'FutureInformationIndicator': True
            }

            res = self.make_request(method='POST', path='GetEmploymentChangedAtDate20070401', params=params)

            root = etree.fromstring(res)

            employments = []
            for p in root.xpath("//Person"):
                cpr = p.find('PersonCivilRegistrationIdentifier').text
                for e in p.xpath('.//Employment'):
                    employment = e.find('EmploymentIdentifier').text
                    # print([date for date in list(set(e.xpath('.//ActivationDate/text()')))])
                    effective_dates = [date.strftime('%Y-%m-%d') for date in sorted([datetime.strptime(date, '%Y-%m-%d') for date in list(set(e.xpath('.//ActivationDate/text()')))])]
                    employment_status_code = e.find('EmploymentStatus/EmploymentStatusCode').text if e.find('EmploymentStatus/EmploymentStatusCode') is not None else None

                    # Only include effective dates after the end date
                    # effective_dates = [date for date in effective_dates if datetime.strptime(date, '%Y-%m-%d') > datetime.strptime(end_date, '%Y-%m-%d')]

                    # If not future date then include the current date
                    if not effective_dates:
                        effective_dates = [end_date]

                    employments.append({'institution': institution_id, 'cpr': cpr, 'employment_id': employment, 'effective_dates': effective_dates, 'employement_status_code': employment_status_code})

            return employments

        except Exception as p:
            logger.error(p)
            return None

    def get_employment_start_date(self, institution_id, cpr_id, effective_date=None):
        try:
            effective_date = datetime.now(pytz.timezone("Europe/Copenhagen")) if effective_date is None else effective_date

            params = {
                'InstitutionIdentifier': institution_id,
                'PersonCivilRegistrationIdentifier': cpr_id,
                # 'EmploymentIdentifier': employment_id,
                'EffectiveDate': effective_date.strftime("%Y-%m-%d"),
                'StatusActiveIndicator': True,
                'EmploymentStatusIndicator': True
            }

            res = self.make_request(method='POST', path='GetEmployment20070401', params=params)

            root = etree.fromstring(res)

            persons = root.xpath("//Person")

            if len(persons) == 1:
                p = persons[0]
                emp_dates = p.findall('.//Employment/EmploymentDate')
                employment_dates = [ed.text for ed in emp_dates if ed is not None and ed.text]
                return employment_dates if employment_dates else None
            elif len(persons) == 0:
                return None
            else:
                raise Exception('Multiple results found')
        except Exception as e:
            logger.error(e)
            return None

    def get_employment_status_and_end_date(self, institution_id, cpr_id, employee_id, effective_date=None):
        try:
            effective_date = datetime.now(pytz.timezone("Europe/Copenhagen")).strftime("%Y-%m-%d") if effective_date is None else effective_date

            params = {
                'InstitutionIdentifier': institution_id,
                'EmploymentIdentifier': employee_id,
                'PersonCivilRegistrationIdentifier': cpr_id,
                'EffectiveDate': effective_date,
                'StatusActiveIndicator': True,
                'ProfessionIndicator': True,
                'DepartmentIndicator': True,
                'EmploymentStatusIndicator': True
            }

            res = self.make_request(method='POST', path='GetEmployment20070401', params=params)

            root = etree.fromstring(res)

            persons = root.xpath("//Person")

            if len(persons) == 1:
                p = persons[0]
                employment = {}

                department = p.find('Employment/Department/DepartmentIdentifier')
                employment['department'] = department.text if department is not None else None

                job_position = p.find('Employment/Profession/JobPositionIdentifier')
                employment['job_position'] = job_position.text if job_position is not None else None

                status_code = p.find('Employment/EmploymentStatus/EmploymentStatusCode')
                employment['employement_status_code'] = status_code.text if status_code is not None else None

                activation_date = p.find('Employment/EmploymentStatus/ActivationDate')
                employment['employment_status_activation_date'] = activation_date.text if activation_date is not None else None

                deactivation_date = p.find('Employment/EmploymentStatus/DeactivationDate')
                employment['employment_status_deactivation_date'] = deactivation_date.text if deactivation_date is not None else None

                return employment
            elif len(persons) == 0:
                logger.warning('No results found')
            else:
                raise Exception('Multiple results found')
        except Exception as e:
            logger.error(e)
            return None
