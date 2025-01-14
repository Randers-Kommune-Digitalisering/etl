from sensum.sensum_data import get_sensum_data
import logging
logger = logging.getLogger(__name__)


def job():
    try:
        logger.info('Starting Sensum ETL job!')
        get_sensum_data()
        return True

    except Exception as e:
        logger.error(f'An error occurred: {e}')
        return False
