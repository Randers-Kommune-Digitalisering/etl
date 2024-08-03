import logging
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
            else:
                raise Exception('niveau 0 or niveau 2 not found for profession')

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

    # Returns a tuple with the department id and the profession id
    def get_employment_details(self, institution_id, cpr_id, employee_id, effective_date=datetime.now()):
        try:
            effective_date = datetime.now().strftime("%Y-%m-%d") if effective_date is None else effective_date
            params = {
                'InstitutionIdentifier': institution_id,
                'EmploymentIdentifier': employee_id,
                'PersonCivilRegistrationIdentifier': cpr_id,
                'EffectiveDate': effective_date,
                'StatusActiveIndicator': True,
                'StatusPassiveIndicator': True,
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

                employment_date = p.find('Employment/EmploymentDate')
                employment['employment_date'] = employment_date.text if employment_date is not None else None

                department = p.find('Employment/Department/DepartmentIdentifier')
                employment['department'] = department.text if department is not None else None

                job_position = p.find('Employment/Profession/JobPositionIdentifier')
                employment['job_position'] = job_position.text if job_position is not None else None

                status_code = p.find('Employment/EmploymentStatus/EmploymentStatusCode')
                employment['status_code'] = status_code.text if status_code is not None else None

                start_date = p.find('Employment/EmploymentStatus/ActivationDate')
                employment['start_date'] = start_date.text if start_date is not None else None

                end_date = p.find('Employment/EmploymentStatus/DeactivationDate')
                employment['end_date'] = end_date.text if end_date is not None else None

                return employment
            elif len(persons) == 0:
                return None
            else:
                raise Exception('Multiple results found')
        except Exception as p:
            logger.error(p)
            return None

    def get_employments_with_changes(self, institution_id, start_datetime=datetime.now() - timedelta(minutes=5), end_datetime=datetime.now()):
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
                'DepartmentIndicator': True,
                'EmploymentStatusIndicator': True,
                'ProfessionIndicator': True
            }

            res = self.make_request(method='POST', path='GetEmploymentChangedAtDate20070401', params=params)

            root = etree.fromstring(res)

            employments = []
            for e in root.xpath("//Person"):
                cpr = e.find('PersonCivilRegistrationIdentifier').text
                employment = e.find('Employment/EmploymentIdentifier').text
                date = e.find('Employment/EmploymentStatus/ActivationDate').text if e.find('Employment/EmploymentStatus/ActivationDate') is not None else None

                employments.append({'cpr': cpr, 'employment_id': employment, 'effective_date': date})

            return employments

        except Exception as e:
            logger.error(e)
            return None
