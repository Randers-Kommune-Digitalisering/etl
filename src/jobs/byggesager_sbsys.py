import urllib.parse

from datetime import datetime, timedelta

from utils.config import CONFIG_LIBRARY_URL, CONFIG_LIBRARY_USER, CONFIG_LIBRARY_PASS, CONFIG_LIBRARY_BASE_PATH, BYGGESAGER_CONFIG_FILE
from utils.api_requests import APIClient
from utils.logging import logging
from byggesager.byggesager import get_sbsys_data, check_up_to_date, update_csv_file, combine_two_csv_files, check_csv_file_for_string
from custom_data_connector import read_data_from_custom_data_connector, post_data_to_custom_data_connector


logger = logging.getLogger(__name__)

config_library_client = APIClient(base_url=CONFIG_LIBRARY_URL, username=CONFIG_LIBRARY_USER, password=CONFIG_LIBRARY_PASS)


def job():
    config_path = urllib.parse.urljoin(CONFIG_LIBRARY_BASE_PATH, BYGGESAGER_CONFIG_FILE)
    config = config_library_client.make_request(path=config_path)
    if not config:
        logging.error(f"Failed to load config file: {BYGGESAGER_CONFIG_FILE}")
        return False

    last_month = (datetime.now().replace(day=1) - timedelta(days=1)).replace(day=1)
    month_before_last = (last_month - timedelta(days=1)).replace(day=1)

    last_month = last_month.strftime('%d-%m-%Y')
    month_before_last = month_before_last.strftime('%d-%m-%Y')

    files_updated = []
    for file_to_update in config['files_to_update']:
        old_file = read_data_from_custom_data_connector(file_to_update, path='in')
        if check_up_to_date(old_file, [last_month, month_before_last]):
            logger.info(f'{old_file.filename} already up to date')
            files_updated.append(True)

    if all(files_updated) and len(files_updated) == len(config['files_to_update']):
        logger.info('All files already up to date')
        return True

    new_data, update_data = get_sbsys_data(config)

    if new_data and update_data:
        results = []
        for file_to_update in config['files_to_update']:
            if old_file:
                for key in new_data:
                    old_file = read_data_from_custom_data_connector(file_to_update, path='in')
                    if key in old_file.filename:
                        if update_data.get(key, {}).get('file', None) and new_data.get(key, {}).get('file', None):
                            update_date = update_data.get(key, {}).get('date', None)
                            if update_date:
                                updated_file = update_csv_file(old_file, update_data[key]['file'], update_date)

                                new_date = new_data.get(key, {}).get('date', None)
                                if new_date:
                                    if check_csv_file_for_string(updated_file, new_date):
                                        logger.info(f'{old_file.filename} already up to date - skipping')
                                        results.append(True)
                                        continue

                                    new_file = combine_two_csv_files(updated_file, new_data[key]['file'])
                                    if post_data_to_custom_data_connector(key, new_file):
                                        logger.info(f'{key} uploaded to custom-data-connector - {file_to_update} updated')
                                        results.append(True)
                                        continue
                                    else:
                                        logger.warning(f'{key} failed to upload to custom-data-connector - {file_to_update} NOT updated')
                                else:
                                    logger.warning('No new date provided')
                                    results.append(False)
                                    continue
                            else:
                                logger.warning('No update date provided')
                                results.append(False)
                                continue
            elif old_file is None:
                for key in new_data:
                    if key in file_to_update:
                        logger.info(f'{file_to_update} does not exist in custom-data-connector - creating')
                        if new_data.get(key, {}).get('file', None) and update_data.get(key, {}).get('date', None):
                            file = combine_two_csv_files(update_data[key]['file'], new_data[key]['file'],)
                            if post_data_to_custom_data_connector(key, file):
                                logger.info(f'{key} uploaded to custom-data-connector - {file_to_update} created')
                                results.append(True)
                                continue
                            else:
                                logger.warning(f'{key} failed to upload to custom-data-connector - {file_to_update} NOT created')
                                results.append(False)
                                continue
                        else:
                            logger.warning(f'Missing file for {key} - {file_to_update} NOT created')
                            results.append(False)
                            continue
            else:
                logger.error('Custom-data-connector failed')
                return False
    else:
        logger.error('UMT SBSYS SFTP failed')
        return False

    return all(results)
