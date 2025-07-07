import logging

from sqlalchemy import true

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

    # Takes cpr and returns a list of dictionaries with employment_id, institution_code and cpr
    def get_engagement_without_user(self, cpr, from_date):
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
                    "validDate": f"{from_date}",
                    "limit": 10
                }
            ]
        }

        res = self.make_request(path='/api/object/graph-query', method='POST', json=query)

        instances = res.get('graphQueryResult', [{}])[0].get('instances', [])

        if len(instances) == 0:
            logger.info("No engagement found in Delta")
            return
        elif len(instances) == 1:
            instance = instances[0]
            engagement_userkey = instance.get('identity', {}).get('userKey', '')
            employment_id = engagement_userkey.split('.')[1] if '.' in engagement_userkey else None
            institution_code = engagement_userkey[:2]

            in_type_refs = instance.get('inTypeRefs', [])
            has_apos_types_user = any(ref.get('refObjTypeUserKey') == 'APOS-Types-User' for ref in in_type_refs)

            if not has_apos_types_user:
                return [{'employment_id': employment_id, 'institution_code': institution_code, 'cpr': cpr}]
            else:
                logger.info("Only engagement has user")
                pass
        else:
            filtered_instances = [
                inst for inst in instances
                if not any(ref.get('refObjTypeUserKey') == 'APOS-Types-User' for ref in inst.get('inTypeRefs', []))
            ]
            if len(filtered_instances) == 0:
                logger.info("All engagements have APOS-Types-User")
                return []
            elif len(filtered_instances) == 1:
                instance = filtered_instances[0]
                engagement_userkey = instance.get('identity', {}).get('userKey', '')
                employment_id = engagement_userkey.split('.')[1] if '.' in engagement_userkey else None
                institution_code = engagement_userkey[:2]
                return [{'employment_id': employment_id, 'institution_code': institution_code, 'cpr': cpr}]
            elif len(filtered_instances) > 1:
                logger.info("Multiple engagements without APOS-Types-User")
                engagements_with_users = []
                for inst in filtered_instances:
                    engagement_userkey = inst.get('identity', {}).get('userKey', '')
                    employment_id = engagement_userkey.split('.')[1] if '.' in engagement_userkey else None
                    institution_code = engagement_userkey[:2]
                    engagements_with_users.append({'employment_id': employment_id, 'institution_code': institution_code, 'cpr': cpr})
                return engagements_with_users
            else:
                logger.error("How can it be negative length?")

    def get_dq_numbers(self, cpr, from_date):
        if not isinstance(from_date, str):
            logger.warning("from_date is not a string: %s", from_date)
            logger.warning(type(from_date))
            return None, []
        if from_date and '.' in from_date:
            try:
                from_date = datetime.strptime(from_date, "%d.%m.%Y").strftime("%Y-%m-%d")
            except ValueError:
                try:
                    from_date = datetime.strptime(from_date, "%d.%m.%y").strftime("%Y-%m-%d")
                except ValueError:
                    logger.warning("from_date format is invalid: %s", from_date)
                    return None, []

        query = {
            "graphQueries": [
                {
                    "computeAvailablePages": False,
                    "graphQuery": {
                        "structure": {
                            "alias": "person",
                            "userKey": "APOS-Types-Person",
                            "relations": [
                                {
                                    "alias": "user",
                                    "title": "APOS-Types-User-TypeRelation-Person",
                                    "userKey": "APOS-Types-User-TypeRelation-Person",
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
                                        "alias": "person.$userKey"
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
                                        "alias": "person.$state"
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
                            "incomingTypeRelations": [
                                {
                                    "userKey": "APOS-Types-User-TypeRelation-Person",
                                    "projection": {
                                        "identity": True,
                                        "state": True
                                    }
                                }
                            ]
                        }
                    },
                    "validDate": f"{from_date}",
                    "limit": 10
                }
            ]
        }

        res = self.make_request(path='/api/object/graph-query', method='POST', json=query)

        instances = res.get('graphQueryResult', [{}])[0].get('instances', [])

        is_in_delta = False

        if len(instances) > 0:
            is_in_delta = True

        user_keys = []

        for inst in instances:
            for ref in inst.get('inTypeRefs', []):
                if ref.get('refObjTypeUserKey') == 'APOS-Types-User':
                    if ref.get('targetObject', {}).get('state', None) == 'STATE_ACTIVE':
                        user_keys.append(ref.get('targetObject', {}).get('identity', {}).get('userKey', None))
        if user_keys:
            return is_in_delta, user_keys
        else:
            return is_in_delta, []

    def get_all_active_engagements(self):
        results = []
        offset = 0
        limit = 1000

        while True:
            grapgh_query = {
                "graphQueries": [
                    {
                        "computeAvailablePages": True,
                        "graphQuery": {
                            "structure": {
                                "alias": "employee",
                                "userKey": "APOS-Types-Engagement",
                                "relations": [
                                    {
                                        "alias": "person",
                                        "userKey": "APOS-Types-Engagement-TypeRelation-Person",
                                        "typeUserKey": "APOS-Types-Person",
                                        "direction": "OUT"
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
                                            "alias": "employee.$state"
                                        },
                                        "right": {
                                            "source": "STATIC",
                                            "value": "STATE_ACTIVE"
                                        }
                                    },
                                ]
                            },
                            "projection": {
                                "identity": True,
                                "state": True,
                                "timeline": "FULL",
                                "typeRelations": [
                                    {
                                        "userKey": "APOS-Types-Engagement-TypeRelation-Person",
                                        "projection": {
                                            "identity": True
                                        }
                                    }
                                ]

                            }
                        },
                        "validDate": "NOW",
                        "offset": offset,
                        "limit": limit
                    }
                ]
            }

            res = self.make_request(path='/api/object/graph-query', method='POST', json=grapgh_query)

            graph_result = res["graphQueryResult"][0]
            available_pages = graph_result.get("availablePages", 0)
            instances = graph_result.get("instances", [])
            # instances = res.get('graphQueryResult', [{}])[0].get('instances', [])
            results = []
            for instance in instances:
                identity_userkey = instance.get("identity", {}).get("userKey")
                tjenestenummer = None
                institution_code = None
                if identity_userkey and "." in identity_userkey:
                    tjenestenummer = identity_userkey.split(".")[1]
                    institution_code = identity_userkey[:2]
                for ref in instance.get("typeRefs", []):
                    if ref.get("refObjTypeUserKey") == "APOS-Types-Person":
                        cpr = ref.get("targetObject", {}).get("identity", {}).get("userKey")
                        break
                else:
                    cpr = None
                validity_from = instance.get("validityInterval", {}).get("from")
                validity_to = instance.get("validityInterval", {}).get("to")
                results.append({"institution_code": institution_code, "tjenestenummer": tjenestenummer, "cpr": cpr, "delta_start_date": validity_from, "delta_end_date": validity_to})

            total_instances = available_pages * limit
            if offset + limit >= total_instances or len(instances) < limit:
                break

            offset += limit

        return results

    def get_adm_children(self, top_adm_org_user_key=None):
        grapgh_query = {
            "graphQueries": [
                {
                    "parameterMap": {
                        "admKey": f"{top_adm_org_user_key}" if top_adm_org_user_key else 1
                    },
                    "computeAvailablePages": False,
                    "graphQuery": {
                        "structure": {
                            "alias": "adm",
                            "userKey": "APOS-Types-AdministrativeUnit",
                            # "relations": [
                            #     {
                            #         "alias": "adm",
                            #         "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                            #         "typeUserKey": "APOS-Types-Engagement",
                            #         "direction": "IN"
                            #     }
                            # ]
                        },
                        "parameters": [
                            {
                                "key": "admUuid",
                                "name": "Admin Org uuid"
                            }
                        ],
                        "criteria": {
                            "type": "AND",
                            "criteria": [
                                {
                                    "type": "MATCH",
                                    "operator": "EQUAL",
                                    "left": {
                                        "source": "DEFINITION",
                                        "alias": "adm.$userKey"
                                    },
                                    "right": {
                                        "source": "PARAMETER",
                                        "key": "admKey"
                                    }
                                },
                                {
                                    "type": "MATCH",
                                    "operator": "EQUAL",
                                    "left": {
                                        "source": "DEFINITION",
                                        "alias": "adm.$state"
                                    },
                                    "right": {
                                        "source": "STATIC",
                                        "value": "STATE_ACTIVE"
                                    }
                                }
                            ]
                        },
                        "projection": {
                            "children": {
                                "identity": True,
                                # "attributes": [
                                #         "APOS-Types-AdministrativeUnit-Attribute-SDUnitCodes"
                                # ]
                            }
                        }
                    },
                    "validDate": "NOW",
                    "offset": 0,
                    "limit": 1
                }
            ]
        }

        res = self.make_request(path='/api/object/graph-query', method='POST', json=grapgh_query)

        isinstances = res.get('graphQueryResult', [])[0].get('instances', [])
        adm_orgs = []
        for inst in isinstances:
            for child in inst.get('childrenObjects', []):
                uuid = child.get('identity', {}).get('uuid', '')
                user_key = child.get('identity', {}).get('userKey', '')
                name = child.get('identity', {}).get('name', '')
                adm_orgs.append({
                    'name': name,
                    'userKey': user_key,
                    'uuid': uuid
                })

        return adm_orgs

    # def get_employees_by_adm_org(self, adm_org_name=None):
    #     grapgh_query = {
    #         "graphQueries": [
    #             {
    #                 "parameterMap": {
    #                     "admName": f"{adm_org_name}" if adm_org_name else 'Randers Kommune'
    #                 },
    #                 "computeAvailablePages": False,
    #                 "graphQuery": {
    #                    "structure": {
    #                         "alias": "employee",
    #                         "userKey": "APOS-Types-Engagement",
    #                         "relations": [
    #                             {
    #                                 "alias": "adm",
    #                                 "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
    #                                 "typeUserKey": "APOS-Types-AdministrativeUnit",
    #                                 "direction": "OUT"
    #                             },
    #                             {
    #                                 "alias": "position",
    #                                 "userKey": "APOS-Types-Engagement-TypeRelation-Position",
    #                                 "typeUserKey": "APOS-Types-PositionType",
    #                                 "direction": "OUT"
    #                             }
    #                         ]
    #                     },
    #                     "parameters": [
    #                         {
    #                             "key": "admName",
    #                             "name": "Admin Org Name"
    #                         }
    #                     ],
    #                     "criteria": {
    #                         "type": "AND",
    #                         "criteria": [
    #                             {
    #                                 "type": "MATCH",
    #                                 "operator": "EQUAL",
    #                                 "left": {
    #                                     "source": "DEFINITION",
    #                                     "alias": "employee.adm.$name"
    #                                 },
    #                                 "right": {
    #                                     "source": "PARAMETER",
    #                                     "key": "admName"
    #                                 }
    #                             },
    #                             {
    #                                 "type": "MATCH",
    #                                 "operator": "EQUAL",
    #                                 "left": {
    #                                     "source": "DEFINITION",
    #                                     "alias": "employee.$state"
    #                                 },
    #                                 "right": {
    #                                     "source": "STATIC",
    #                                     "value": "STATE_ACTIVE"
    #                                 }
    #                             }
    #                         ]
    #                     },
    #                     "projection": {
    #                         "identity": True,
    #                         "attributes": [
    #                            "APOS-Types-Engagement-Attribute-Email",
    #                            "APOS-Types-Engagement-Attribute-Mobile",
    #                            "APOS-Types-Engagement-Attribute-SDUnitCode"

    #                         ],
    #                         "typeRelations": [
    #                             {
    #                                 "userKey": "APOS-Types-Engagement-TypeRelation-Position",
    #                                 "projection": {
    #                                     "identity": True
    #                                 }
    #                             }
    #                         ]
    #                     }
    #                 },
    #                 "validDate": "NOW"
    #             }
    #         ]
    #     }

    #     res = self.make_request(path='/api/object/graph-query', method='POST', json=grapgh_query)

    #     instances = res.get('graphQueryResult', [])[0].get('instances', [])

    #     employees = []

    #     for inst in instances:
    #         institution_code = inst.get('identity', {}).get('userKey', '')[:2]
    #         name = inst.get('identity', {}).get('name', '')
    #         tjenestenummer = inst.get('identity', {}).get('userKey', '').split('.')[1] if '.' in inst.get('identity', {}).get('userKey', '') else None
    #         birthday = inst.get('identity', {}).get('userKey', '').split('.')[-1] if '.' in inst.get('identity', {}).get('userKey', '') else None
    #         attributes = inst.get('attributes', [])
    #         email = next((att['value'] for att in attributes if att['userKey'] == 'APOS-Types-Engagement-Attribute-Email'), None)
    #         mobile = next((att['value'] for att in attributes if att['userKey'] == 'APOS-Types-Engagement-Attribute-Mobile'), None)
    #         sd_unit_code = next((att['value'] for att in attributes if att['userKey'] == 'APOS-Types-Engagement-Attribute-SDUnitCode'), None)

    #         titel = inst.get('typeRefs', [{}])[0].get('targetObject', {}).get('identity', {}).get('userKey', '')

    #         try:
    #             dt = datetime.strptime(birthday, "%d%m%y")
    #             year = dt.year
    #             today = datetime.today()
    #             age = today.year - year - ((today.month, today.day) < (dt.month, dt.day))
    #             if year >= 2000 and age < 18:
    #                 dt = dt.replace(year=year - 100)
    #             birthday_formatted = dt.strftime("%d-%m-%Y")
    #         except Exception:
    #             birthday_formatted = birthday

    #         employees.append({
    #             'institution_code': institution_code,
    #             'Navn': name,
    #             'Personalenr.': tjenestenummer,
    #             'Mobiltelefonnr.': mobile,
    #             'Email': email,
    #             'MasterGroup': adm_org_name,
    #             'UserGroup': sd_unit_code,
    #             'Titel': titel,
    #             'Fødselsdag': birthday_formatted
    #         })

    #     return employees

    def get_employees_by_sd_department(self, sd_department_id: str):
        if not sd_department_id:
            raise ValueError("sd_department_id must be provided")
        grapgh_query = {
            "graphQueries": [
                {
                    "parameterMap": {
                        "sdid": sd_department_id
                    },
                    "computeAvailablePages": False,
                    "graphQuery": {
                        "structure": {
                            "alias": "employee",
                            "userKey": "APOS-Types-Engagement",
                            "attributes": [
                                {
                                    "alias": "sddep",
                                    "userKey": "APOS-Types-Engagement-Attribute-SDUnitCode"
                                }
                            ],
                            "relations": [
                                {
                                    "alias": "adm",
                                    "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                    "typeUserKey": "APOS-Types-AdministrativeUnit",
                                    "direction": "OUT"
                                },
                                {
                                    "alias": "position",
                                    "userKey": "APOS-Types-Engagement-TypeRelation-Position",
                                    "typeUserKey": "APOS-Types-PositionType",
                                    "direction": "OUT"
                                }
                            ]
                        },
                        "parameters": [
                            {
                                "key": "sdid",
                                "name": "SD Department Id"
                            }
                        ],
                        "criteria": {
                            "type": "AND",
                            "criteria": [
                                {
                                    "type": "MATCH",
                                    "operator": "EQUAL",
                                    "left": {
                                        "source": "DEFINITION",
                                        "alias": "employee.sddep"
                                    },
                                    "right": {
                                        "source": "PARAMETER",
                                        "key": "sdid"
                                    }
                                },
                                {
                                    "type": "MATCH",
                                    "operator": "EQUAL",
                                    "left": {
                                        "source": "DEFINITION",
                                        "alias": "employee.$state"
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
                            "attributes": [
                                "APOS-Types-Engagement-Attribute-Email",
                                "APOS-Types-Engagement-Attribute-Mobile",
                                "APOS-Types-Engagement-Attribute-SDUnitCode"
                            ],
                            "typeRelations": [
                                {
                                    "userKey": "APOS-Types-Engagement-TypeRelation-Position",
                                    "projection": {
                                        "identity": True
                                    }
                                },
                                {
                                    "userKey": "APOS-Types-Engagement-TypeRelation-AdmUnit",
                                    "projection": {
                                        "identity": True
                                    }
                                }
                            ]
                        }
                    },
                    "validDate": "NOW"
                }
            ]
        }

        res = self.make_request(path='/api/object/graph-query', method='POST', json=grapgh_query)

        instances = res.get('graphQueryResult', [])[0].get('instances', [])

        employees = []

        for inst in instances:
            sd_department_id_delta = next((att['value'] for att in inst.get('attributes', []) if att['userKey'] == 'APOS-Types-Engagement-Attribute-SDUnitCode'), None)
            if sd_department_id_delta == sd_department_id:
                # institution_code = inst.get('identity', {}).get('userKey', '')[:2]
                name = inst.get('identity', {}).get('name', '')
                tjenestenummer = inst.get('identity', {}).get('userKey', '').split('.')[1] if '.' in inst.get('identity', {}).get('userKey', '') else None
                birthday = inst.get('identity', {}).get('userKey', '').split('.')[-1] if '.' in inst.get('identity', {}).get('userKey', '') else None

                attributes = inst.get('attributes', [])
                email = next((att['value'] for att in attributes if att['userKey'] == 'APOS-Types-Engagement-Attribute-Email'), None)
                mobile = next((att['value'] for att in attributes if att['userKey'] == 'APOS-Types-Engagement-Attribute-Mobile'), None)

                type_refs = inst.get('typeRefs', [])
                titel = next((ref for ref in type_refs if ref.get('refObjTypeUserKey') == 'APOS-Types-PositionType'), {}).get('targetObject', {}).get('identity', {}).get('userKey', '')
                adm_org_name = next((ref for ref in type_refs if ref.get('refObjTypeUserKey') == 'APOS-Types-AdministrativeUnit'), {}).get('targetObject', {}).get('identity', {}).get('name', '')

                try:
                    dt = datetime.strptime(birthday, "%d%m%y")
                    year = dt.year
                    today = datetime.today()
                    age = today.year - year - ((today.month, today.day) < (dt.month, dt.day))
                    if year >= 2000 and age < 18:
                        dt = dt.replace(year=year - 100)
                    birthday_formatted = dt.strftime("%d-%m-%Y")
                except Exception:
                    birthday_formatted = birthday

                employees.append({
                    # 'institution_code': institution_code,
                    'Navn': name,
                    'Personalenr.': tjenestenummer,
                    'Mobiltelefonnr.': mobile,
                    'Email': email,
                    'MasterGroup': adm_org_name,
                    'UserGroup': sd_department_id,
                    'Titel': titel,
                    'Fødselsdag': birthday_formatted
                })
            else:
                logger.warning(f"SD Department ID mismatch: delta: {sd_department_id_delta}, sd: {sd_department_id}")

        return employees
