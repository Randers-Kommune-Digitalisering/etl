import logging
import urllib.parse
from sensum.sensum import (
    handle_files, merge_dataframes,
    process_sensum, create_merge_lambda,
    sags_aktiviteter_merge_df, sensum_data_merge_df,
    merge_df_ydelse, merge_df_sensum_mål)
from utils.api_requests import APIClient
from utils.config import CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_URL, CONFIG_LIBRARY_BASE_PATH, SENSUM_CONFIG_FILE

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
            process_func = globals().get(config.get('process_func'))
            merge_func = globals().get(config.get('merge_func'))
            if process_func is None:
                logging.error(f"Failed to find process function: {config.get('process_func')}")
                return False
            if merge_func is None:
                logging.error(f"Failed to find merge function: {config.get('merge_func')}")
                return False

            merge_lambda = create_merge_lambda(merge_func, config)

            results.append(process_sensum(
                config['patterns'],
                config['directories'],
                process_func,
                merge_lambda,
                config['name']
            ))
        return all(results)
    except Exception as e:
        logger.error(f'An error occurred: {e}')
        return False
