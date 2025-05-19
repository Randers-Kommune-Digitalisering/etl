import logging

from utils.api_requests import APIClient
from datetime import datetime

logger = logging.getLogger(__name__)


class DeltaClient(APIClient):
    def __init__(self, base_url, auth_url, realm, client_id, client_secret, add_auth_to_path=False):
        super().__init__(base_url=base_url, auth_url=auth_url, realm=realm, client_id=client_id, client_secret=client_secret, add_auth_to_path=add_auth_to_path)

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

    # Takes and returns dates as strings in the format YYYY-MM-DD
    def get_engagement(self, cpr):
        query = {
            "graphQueries": [
                {
                    "computeAvailablePages": False,
                    "graphQuery": {
                        "structure": {
                            "alias": "eng",
                            "userKey": "APOS-Types-Engagement",
                            "relations": [
                                {
                                    "alias": "person",
                                    "title": "APOS-Types-Engagement-TypeRelation-Person",
                                    "userKey": "APOS-Types-Engagement-TypeRelation-Person",
                                    "typeUserKey": "APOS-Types-Person",
                                    "direction": "OUT"
                                },
                                {
                                    "alias": "user",
                                    "title": "APOS-Types-User-TypeRelation-Engagement",
                                    "userKey": "APOS-Types-User-TypeRelation-Engagement",
                                    "typeUserKey": "APOS-Types-User",
                                    "direction": "IN"
                                }
                            ]
                        },
                        "criteria": {
                            "type": "AND",
                            "criteria": [
                                {
                                    "type": "MATCH",
                                    "operator": "EQUAL",
                                    "left": {
                                        "source": "DEFINITION",
                                        "alias": "eng.person.$userKey"
                                    },
                                    "right": {
                                        "source": "STATIC",
                                        "value": f"{cpr}"
                                    }
                                },
                                {
                                    "type": "MATCH",
                                    "operator": "EQUAL",
                                    "left": {
                                        "source": "DEFINITION",
                                        "alias": "eng.$state"
                                    },
                                    "right": {
                                        "source": "STATIC",
                                        "value": "STATE_ACTIVE"
                                    }
                                }
                            ]
                        },
                        "projection": {
                            "identity": True,
                            "state": True,
                            "timeline": "FULL",
                            "attributes": [
                                "APOS-Types-Engagement-Attribute-SDUnitCode"
                            ],
                            "incomingTypeRelations": [
                                {
                                    "userKey": "APOS-Types-User-TypeRelation-Engagement",
                                    "projection": {
                                        "identity": True
                                    }
                                }
                            ]
                        }
                    },
                    "validDate": "NOW",
                    "limit": 10
                }
            ]
        }

        res = self.make_request(path='/api/object/graph-query', method='POST', json=query)

        instances = res.get('graphQueryResult', [{}])[0].get('instances', [])

        if len(instances) == 1:
            instance = instances[0]
            engagement_userkey = instance.get('identity', {}).get('userKey', '')
            employment_id = engagement_userkey.split('.')[1] if '.' in engagement_userkey else None
            institution_code = engagement_userkey[:2]

            in_type_refs = instance.get('inTypeRefs', [])
            has_apos_types_user = any(ref.get('refObjTypeUserKey') == 'APOS-Types-User' for ref in in_type_refs)

            if not has_apos_types_user:
                return {'employment_id': employment_id, 'institution_code': institution_code, 'cpr': cpr}
            else:
                logger.info("Engagement has user")
                pass

        else:
            if len(instances) == 0:
                logger.info("No engagement found in Delta")
                pass
            else:
                logger.warning("Many engagements returned from Delta")
                pass


            # if has_apos_types_user:
            #     user_refs = [ref for ref in in_type_refs if ref.get('refObjTypeUserKey') == 'APOS-Types-User']
            #     print(len(user_refs))
            #     for user_ref in user_refs:
            #         if user_ref:
            #             dqnummer = user_ref.get('targetObject', {}).get('identity', {}).get('userKey', None)
            #             print(dqnummer)

    # def get_current_valid_period_dict(timeline, today=None):
    #     if today is None:
    #         today = datetime.today().date()
    #     for period in timeline:
    #         from_date = datetime.strptime(period['from'], "%Y-%m-%d").date()
    #         to_str = period['to']
    #         if to_str == "PLUS_INF":
    #             to_date = datetime(9999, 12, 31).date()
    #             to_formatted = "31.12.9999"
    #         else:
    #             to_date = datetime.strptime(to_str, "%Y-%m-%d").date()
    #             to_formatted = to_date.strftime("%d.%m.%Y")
    #         if from_date <= today < to_date:
    #             return {
    #                 "Startdato": from_date.strftime("%d.%m.%Y"),
    #                 "Slutdato": to_formatted
    #             }
    #     return None
