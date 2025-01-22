import logging
from sensum.sensum import process_sensum, handle_files, merge_dataframes
from sensum.sensum_data import get_sensum_data
from sensum.sensum_sags_aktiviteter import get_sensum_sags_aktiviteter
from utils.api_requests import APIClient
from utils.config import CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_URL, SENSUM_CONFIG_LIBRARY_PATH
from sensum.sensum_ydelse import get_sensum_ydelse
from sensum.sensum_administrative_forhold import get_sensum_adminstrative_forhold

logger = logging.getLogger(__name__)

config_library_client = APIClient(base_url=CONFIG_LIBRARY_URL, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)


def job():
    try:
        logger.info('Starting Sensum ETL job!')

        sensum_jobs_config = config_library_client.make_request(path=SENSUM_CONFIG_LIBRARY_PATH)
        if sensum_jobs_config is None:
            logging.error(f"Failed to load config file from path: {SENSUM_CONFIG_LIBRARY_PATH}")
            return False

        results = []
        for config in sensum_jobs_config:
            results.append(process_sensum(
                config['patterns'],
                handle_files,
                lambda *dfs: merge_dataframes(
                    *dfs, config['merge_on'], config['group_by'], config['agg_columns'], config['columns']
                ),
                config['name']
            ))
        results.append(get_sensum_sags_aktiviteter())
        results.append(get_sensum_data())
        results.append(get_sensum_ydelse())
        results.append(get_sensum_adminstrative_forhold())
        return all(results)
    except Exception as e:
        logger.error(f'An error occurred: {e}')
        return False
