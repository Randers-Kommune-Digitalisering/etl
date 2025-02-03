import urllib.parse
import logging

from jobindsats.jobindsats import get_data
from utils.api_requests import APIClient
from utils.config import CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_URL, CONFIG_LIBRARY_BASE_PATH, JOBINDSATS_CONFIG_FILE

logger = logging.getLogger(__name__)

config_library_client = APIClient(base_url=CONFIG_LIBRARY_URL, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)


def job():
    try:
        logger.info('Starting jobindsats ETL job!')

        config_path = urllib.parse.urljoin(CONFIG_LIBRARY_BASE_PATH, JOBINDSATS_CONFIG_FILE)
        jobindsats_jobs_config = config_library_client.make_request(path=config_path)
        if jobindsats_jobs_config is None:
            logging.error(f"Failed to load config file from path: {config_path}")
            return False

        results = []
        for job in jobindsats_jobs_config:
            results.append(get_data(job['name'], job['years_back'], job['dataset'], job['period_format'], job['data_to_get']))
        return all(results)
    except Exception as e:
        logger.error(f'An error occurred: {e}')
        return False
