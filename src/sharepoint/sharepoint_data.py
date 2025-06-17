from utils.api_requests import APIClient
import logging
from utils.config import API_SERVICE_URL

logger = logging.getLogger(__name__)


def get_sharepoint_list():
    try:
        client = APIClient(base_url=API_SERVICE_URL)
        logger.info(f"Base URL: {client.base_url}")
        response = client.make_request(path="/api/azure/sharepoint-list", method='GET')
        items = response.get("items", [])
        return items
    except Exception as e:
        logger.info(f"Could not get SharePoint list: {e}")
        return []
