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

    def get_all_institutions_df(self):
        try:
            params = {'RegionIdentifier': '9R'}

            res = self.make_request('POST', 'GetInstitution20080201', params=params)

            xml = res.content

            return pd.read_xml(xml, xpath='.//Institution')
        except Exception as e:
            logger.error(e)
            return None
        
    def get_all_departments_df(self, institution_id, activation_date=datetime.now(), deactivation_date=datetime.now()):
        try:
            params = {
                'InstitutionIdentifier': institution_id,
                'ActivationDate': activation_date.strftime("%Y-%m-%d"),
                'DeactivationDate': deactivation_date.strftime("%Y-%m-%d"),
                'DepartmentNameIndicator': True
            }

            res = self.make_request('POST', 'GetDepartment20111201', params=params)

            xml = res.content

            return pd.read_xml(xml, xpath='.//Department')
        except Exception as e:
            logger.error(e)
            return None

    def get_employment_changes_df(self, institution_id, start_datetime=datetime.now() - timedelta(minutes=5), end_datetime=datetime.now()):
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

            res = self.make_request('POST', 'GetEmploymentChanged20111201', params=params)

            xml = res.content
            root = etree.fromstring(xml)

            flattened_data = [flatten_xml(person) for person in root.xpath("//Person")]

            return pd.DataFrame(flattened_data).dropna(how='all', axis=1)
        except Exception as e:
            logger.error(e)
            return None
