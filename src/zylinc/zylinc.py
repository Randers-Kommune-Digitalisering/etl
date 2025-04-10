import logging

logger = logging.getLogger(__name__)


def query_elasticsearch(es_client, scroll_size, body):
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


def fetch_data_from_elasticsearch(queue_name, es_client, scroll_size=1000):
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

        data_to_insert = query_elasticsearch(es_client, scroll_size, body)
        return data_to_insert
    except Exception as e:
        logger.error(f"Error fetching data from Elasticsearch for queue {queue_name}: {e}")
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
        "Jobcenter_Team Integration_7738"
    ]
    return queue_names
