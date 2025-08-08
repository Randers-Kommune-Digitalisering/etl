import logging

logger = logging.getLogger(__name__)


def query_queue_elasticsearch(es_client, scroll_size, body):
    try:
        all_hits = es_client.get_all_hits(index="conversation-events-1", body=body, scroll='2m', size=scroll_size)

        data_to_insert = []
        for hit in all_hits:
            source = hit['_source']
            formatted_start_time = hit['fields']['FormattedStartTimeUtc'][0] if 'fields' in hit and 'FormattedStartTimeUtc' in hit['fields'] else None
            data_to_insert.append({
                "QueueName": source.get("QueueName"),
                "Result": source.get("Result"),
                "AgentDisplayName": source.get("AgentDisplayName"),
                "ConversationEventType": source.get("ConversationEventType"),
                "StartTimeUtc": formatted_start_time,
                "TotalDurationInMilliseconds": source.get("TotalDurationInMilliseconds"),
                "EventDurationInMilliseconds": source.get("EventDurationInMilliseconds")
            })

        return data_to_insert
    except Exception as e:
        logger.error(f"Error querying Elasticsearch: {e}")


def fetch_queue_data_from_elasticsearch(queue_name, es_client, scroll_size=1000):
    try:
        logger.info(f"Fetching data from Elasticsearch for queue: {queue_name}")
        body = {
            "_source": ["QueueName", "Result", "AgentDisplayName", "ConversationEventType", "TotalDurationInMilliseconds", "EventDurationInMilliseconds", "ConversationEventType"],
            "query": {
                "match": {
                    "QueueName": queue_name
                }
            },
            "script_fields": {
                "FormattedStartTimeUtc": {
                    "script": {
                        "source": "SimpleDateFormat format = new SimpleDateFormat('yyyy-MM-dd HH:mm:ss'); return format.format(new Date(doc['StartTimeUtc'].value.toInstant().toEpochMilli()));"
                    }
                }
            }
        }

        data_to_insert = query_queue_elasticsearch(es_client, scroll_size, body)
        return data_to_insert
    except Exception as e:
        logger.error(f"Error fetching data from Elasticsearch for queue {queue_name}: {e}")
        return None


def query_activity_data(es_client, scroll_size, body):
    try:
        all_hits = es_client.get_all_hits(index="clientprod-t19n-activity-data-6", body=body, scroll='2m', size=scroll_size)
        data_to_insert = []
        for hit in all_hits:
            source = hit['_source']
            formatted_first_answer_time = hit['fields']['FormattedFirstAnswerTimeUtc'][0] if 'fields' in hit and 'FormattedFirstAnswerTimeUtc' in hit['fields'] else None
            formatted_start_time = hit['fields']['FormattedStartTimeUtc'][0] if 'fields' in hit and 'FormattedStartTimeUtc' in hit['fields'] else None
            data_to_insert.append({
                "FirstQueueDisplayName": source.get("FirstQueueDisplayName"),
                "FirstAnswerAgentDisplayName": source.get("FirstAnswerAgentDisplayName"),
                "LastQueueDisplayName": source.get("LastQueueDisplayName"),
                "FirstAnswerTimeUtc": formatted_first_answer_time,
                "StartTimeUtc": formatted_start_time,
                "TransferToName": source.get("TransferToName"),
                "Result": source.get("Result")
            })
        return data_to_insert
    except Exception as e:
        logger.error(f"Error querying clientprod-t19n-activity-data-6: {e}")


def fetch_activity_data_from_elasticsearch(es_client, scroll_size=1000):
    try:
        logger.info("Fetching data from Elasticsearch index: clientprod-t19n-activity-data-6")
        body = {
            "_source": [
                "FirstQueueDisplayName",
                "FirstAnswerAgentDisplayName",
                "LastQueueDisplayName",
                "FirstAnswerTimeUtc",
                "StartTimeUtc",
                "TransferToName",
                "Result"
            ],
            "query": {
                "bool": {
                    "must": [
                        { "match": { "FirstQueueDisplayName": "Hovednummer_89151515" } }
                    ],
                    "must_not": [
                        { "match": { "LastQueueDisplayName": "Omstillingen" } },
                        { "match": { "LastQueueDisplayName": "Hovednummer_89151515" } }
                    ]
                }
            },
            "script_fields": {
                "FormattedFirstAnswerTimeUtc": {
                    "script": {
                        "source": """
                            if (doc.containsKey('FirstAnswerTimeUtc') && !doc['FirstAnswerTimeUtc'].empty) {
                                SimpleDateFormat format = new SimpleDateFormat('yyyy-MM-dd HH:mm:ss');
                                return format.format(new Date(doc['FirstAnswerTimeUtc'].value.toInstant().toEpochMilli()));
                            } else {
                                return null;
                            }
                        """
                    }
                },
                "FormattedStartTimeUtc": {
                    "script": {
                        "source": """
                            if (doc.containsKey('StartTimeUtc') && !doc['StartTimeUtc'].empty) {
                                SimpleDateFormat format = new SimpleDateFormat('yyyy-MM-dd HH:mm:ss');
                                return format.format(new Date(doc['StartTimeUtc'].value.toInstant().toEpochMilli()));
                            } else {
                                return null;
                            }
                        """
                    }
                }
            }
        }
        data_to_insert = query_activity_data(es_client, scroll_size, body)
        return data_to_insert
    except Exception as e:
        logger.error(f"Error fetching data from clientprod-t19n-data: {e}")
        return None


def get_queue_names():
    queue_names = [
        "IT_Digitalisering_1818",
        "Jobcenter Randers",
        "Jobcenter_Fleksgruppen_7734",
        "Jobcenter_Jobservice_7733",
        "Jobcenter_JobogTilknytning_7732",
        "Jobcenter_Udviklingshuset_7735",
        "Jobcenter_Sygedagpenge_7732",
        "Jobcenter_Team Integration_7738",
        "Hovednummer_89151515",
        "TM_Byggesag_5100",
        "Borgerservice_Boliglån_1984",
        "Borgerservice_Folkeregister_1978",
        "Borgerservice_Pas_Korekort_89159000",
        "Borgerservice_Pension_89151986",
        "Borgerservice_Team Information_89159001",
        "Omstillingen"
    ]
    return queue_names
