import logging
import urllib.parse
from utils.api_requests import APIClient
from utils.config import CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_URL, CONFIG_LIBRARY_BASE_PATH, SENSUM_CONFIG_FILE
from sensum.sensum import create_merge_lambda, process_sensum

logger = logging.getLogger(__name__)

config_library_client = APIClient(base_url=CONFIG_LIBRARY_URL, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)


def job():
    try:
        logger.info('Starting Sensum ETL job!')
        config_path = urllib.parse.urljoin(CONFIG_LIBRARY_BASE_PATH, SENSUM_CONFIG_FILE)

        logger.info(f'Config path: {config_path}')
        sensum_jobs_config = config_library_client.make_request(path=config_path)

        if sensum_jobs_config is None:
            logging.error(f"Failed to load config file from path: {config_path}")
            return False

        results = []
        for config in sensum_jobs_config:
            merge_lambda = create_merge_lambda(config)

            results.append(process_sensum(
                config['patterns'],
                config['directories'],
                merge_lambda,
                config['name']
            ))
        return all(results)
    except Exception as e:
        logger.error(f'An error occurred: {e}')
        return False
