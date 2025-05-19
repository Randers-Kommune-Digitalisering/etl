import logging
import pandas as pd

from io import StringIO
from requests import Session


logger = logging.getLogger(__name__)


class LogivaSignflowClient:
    def __init__(self, base_url, username=None, password=None):
        self.base_url = base_url.rstrip('/')
        self.username = username
        self.password = password

    def get_it_department_authorizations_df(self):
        with Session() as session:
            try:
                # login
                endpoint = f'{self.base_url}/usr/auth/basic'
                print(endpoint)
                print(self.username)
                print(self.password)
                res = session.get(endpoint)
                res.raise_for_status()

                endpoint = f'{self.base_url}/usr/auth/j_security_check'
                res = session.post(endpoint, data={'j_username': self.username, 'j_password': self.password})
                res.raise_for_status()

                # get it department authorizations
                endpoint = f'{self.base_url}/usr/ShowDocument'
                params = {'mode': 0, 'FolderStatus_FolderStatusOid': 373, 'sortOrder': 'd', 'sortcolumn': -1, 'pageBeginning': 0, 'csv': 'true'}
                # returns html on first request - ignore response
                res = session.get(endpoint, params=params)
                res.raise_for_status()
                # returns csv on second request
                res = session.get(endpoint, params=params)
                res.raise_for_status()

                # parse csv to dataframe
                colnames = ['Name', 'CPR', 'Assigned Login', 'DQ-number', 'From Date', 'LOS', 'Action', 'Creation time', 'Case Number', 'los1', 'los2', 'los3', 'los4', 'los5', 'los6', 'los7', 'los8', 'los9', 'manager email']
                df = pd.read_csv(StringIO(res.content.decode('cp1252')), sep='\t', names=colnames, header=None, index_col=False)

                print(df)

                # logout
                endpoint = f'{self.base_url}/usr/auth/logout'
                session.get(endpoint)

                return df

            except Exception as e:
                logger.error(e)
                return None
