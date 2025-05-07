from elasticsearch8 import Elasticsearch
import logging


logger = logging.getLogger(__name__)


class ElasticSearchClient:
    def __init__(self, host, port, scheme, username, password):
        self.es = Elasticsearch(
            hosts=[{'host': host, 'port': int(port), 'scheme': scheme}],
            basic_auth=(username, password)
        )
        self.logger = logging.getLogger(__name__)

    def test_connection(self):
        try:
            if self.es.ping():
                self.logger.info("Connected to Elasticsearch")
                return True
            else:
                self.logger.error("Could not connect to Elasticsearch")
                return False
        except Exception as e:
            self.logger.error(f"Error connecting to Elasticsearch: {e}")
            return False

    def search(self, index, body, scroll, size):
        try:
            response = self.es.search(index=index, body=body, scroll=scroll, size=size)
            return response
        except Exception as e:
            self.logger.error(f"Error querying Elasticsearch: {e}")

    def scroll(self, scroll_id, scroll):
        try:
            response = self.es.scroll(scroll_id=scroll_id, scroll=scroll)
            return response
        except Exception as e:
            self.logger.error(f"Error scrolling Elasticsearch: {e}")

    def get_all_hits(self, index, body, scroll, size):
        try:
            response = self.search(index, body, scroll, size)
            scroll_id = response['_scroll_id']
            total_hits = response['hits']['total']['value']
            all_hits = response['hits']['hits']

            while len(response['hits']['hits']) > 0:
                response = self.scroll(scroll_id=scroll_id, scroll=scroll)
                scroll_id = response['_scroll_id']
                all_hits.extend(response['hits']['hits'])

            self.logger.info(f"Retrieved {len(all_hits)} documents out of {total_hits}")
            return all_hits
        except Exception as e:
            self.logger.error(f"Error retrieving all hits from Elasticsearch: {e}")
