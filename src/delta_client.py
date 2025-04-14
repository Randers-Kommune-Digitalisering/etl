import logging

from urllib.parse import urlparse, urlunparse

from utils.api_requests import APIClient
from datetime import datetime

logger = logging.getLogger(__name__)


class DeltaClient(APIClient):
    def __init__(self, base_url, password, cert_base64):
        parsed_url = urlparse(base_url)
        cleaned_base_url = urlunparse((parsed_url.scheme, parsed_url.netloc, '', '', '', ''))
        super().__init__(cleaned_base_url, password=password, cert_base64=cert_base64)

    # returns a tuple with a boolean and the person uuid
    def person_can_deactivate(self, cpr):
        query = {
            "queries": [
                {
                    "criteria": {
                        "identity": {
                            "objUserKey": cpr
                        }
                    },
                    "typeFilter": {
                        "userKey": "APOS-Types-Person"
                    },
                    "resultLimit": {
                        "scopeLimitList": [
                            "IN_TYPE_RELATIONS"
                        ],
                        "limit": 10,
                        "offset": 0
                    },
                    "validDate": "NOW"
                }
            ]
        }

        res = self.make_request(path='/api/object/query', method='POST', json=query)

        if len(res['queryResults']) == 1:
            if len(res['queryResults'][0]['instances']) == 1 and res['queryResults'][0]['instancesCount'] == 1:
                person = res['queryResults'][0]['instances'][0]
                no_realated_objects = len(person['inTypeRefs']) == 0

                return no_realated_objects, person['identity']['uuid']
            else:
                raise Exception("None or many people returned from Delta")
        else:
            raise Exception("Failed to query Delta")

    def person_deactivate(self, uuid):
        query = {
            "transaction": "ALL",
            "objectUpdateList": [
                {
                    "scope": {
                        "flags": [
                            "STATE"
                        ]
                    },
                    "instance": {
                        "validityInterval": {
                            "from": "NOW",
                            "to": "PLUS_INF"
                        },
                        "objTypeUserKey": "APOS-Types-Person",
                        "identity": {
                            "uuid": uuid

                        },
                        "state": "STATE_INACTIVE"
                    }
                }
            ]
        }

        res = self.make_request(path='/api/object/update', method='POST', json=query)

        if res.get('result', {}).get('code', None) == 'OK':
            return True
        else:
            raise Exception(res)

    # Takes and returns dates as strings in the format YYYY-MM-DD
    def get_engagement_start_date_based_on_sd_dates(self, employment_id, date_of_birth, start_date, end_date):
        query = {
            "graphQueries": [
                {
                    "computeAvailablePages": False,
                    "graphQuery": {
                        "structure": {
                            "alias": "eng",
                            "userKey": "APOS-Types-Engagement"
                        },
                        "criteria": {
                            "type": "AND",
                            "criteria": [
                                {
                                    "type": "MATCH",
                                    "operator": "LIKE",
                                    "left": {
                                        "source": "DEFINITION",
                                        "alias": "eng.$userKey"
                                    },
                                    "right": {
                                        "source": "STATIC",
                                        "value": f"%{employment_id}%{date_of_birth}%"
                                    }
                                }
                            ]
                        },
                        "projection": {
                            "identity": True,
                            "state": True,
                            "timeline": "FULL"
                        }
                    },
                    "validDate": "NOW",
                    "limit": 10
                }
            ]
        }

        res = self.make_request(path='/api/object/graph-query', method='POST', json=query)

        if len(res['graphQueryResult']) == 1:
            start_date = datetime.strptime(start_date, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_date, "%Y-%m-%d").date()
            today = datetime.today().date()

            if len(res['graphQueryResult'][0]['instances']) == 1:
                engagement = res['graphQueryResult'][0]['instances'][0]
                engagement_timeline = engagement['timeline']

                engagement_timeline = [{**p, 'to': '9999-12-31'} if p['to'] == 'PLUS_INF' else p for p in engagement_timeline]

                dates_between_start_and_end = [datetime.strptime(p['from'], "%Y-%m-%d").date() for p in engagement_timeline if datetime.strptime(p['from'], "%Y-%m-%d").date() >= start_date and datetime.strptime(p['from'], "%Y-%m-%d").date() <= today and datetime.strptime(p['to'], "%Y-%m-%d").date() <= end_date]

                if len(dates_between_start_and_end) == 0:
                    # No dates which are not in the future and newer than start_date - keeping start_date
                    return start_date.strftime("%Y-%m-%d")
                elif len(dates_between_start_and_end) == 1:
                    d = dates_between_start_and_end[0].strftime("%Y-%m-%d")
                    return d
                elif len(dates_between_start_and_end) > 1:
                    d = sorted(dates_between_start_and_end)
                    return d[-1].strftime("%Y-%m-%d")
            elif len(res['graphQueryResult'][0]['instances']) == 0:
                # No results - probably a new employement
                return None
            else:
                logger.warning("Many engagements returned from Delta for employment_id: %s", employment_id)

    def upload_sd_file(self, file_name, file):
        multipart_form_data = {'file': (file_name, file, 'application/vnd.ms-excel')}

        res = self.make_request(path='/integration-sd/import/run-process', method='POST', files=multipart_form_data)

        if res.get('result', {}).get('code', None) == 'OK':
            return True
        else:
            raise Exception(res)
