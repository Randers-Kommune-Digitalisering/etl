from jobindsats.jobindsats import get_data
import logging
from utils.api_requests import APIClient
from utils.config import CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS

logger = logging.getLogger(__name__)

base_url = "https://config-library.prototypes.randers.dk"
config_library_client = APIClient(base_url, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)


def job():
    try:
        logger.info('Starting jobindsats ETL job!')

        config_path = "/api/file/tests/jobindsats_jobs_config.json"
        jobindsats_jobs_config = config_library_client.make_request(path=config_path)
        if jobindsats_jobs_config is None:
            return False

        results = []
        for job in jobindsats_jobs_config:
            results.append(get_data(job['name'], job['years_back'], job['dataset'], job['period_format'], job['data_to_get']))
        return all(results)
    except Exception as e:
        logger.error(f'An error occurred: {e}')
        return False
