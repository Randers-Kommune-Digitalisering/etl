import logging

from urllib.parse import urlparse, urlunparse

from utils.api_requests import APIClient

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

    def upload_sd_file(self, file_name, file):
        multipart_form_data = {'file': (file_name, file, 'application/vnd.ms-excel')}

        res = self.make_request(path='/integration-sd/import/run-process', method='POST', files=multipart_form_data)

        if res.get('result', {}).get('code', None) == 'OK':
            return True
        else:
            raise Exception(res)
