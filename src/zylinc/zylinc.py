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
