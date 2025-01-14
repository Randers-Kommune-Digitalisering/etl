from sensum.sensum_data import get_sensum_data
from sensum.sensum_sager import get_sensum_sager
import logging
logger = logging.getLogger(__name__)


def job():
    try:
        logger.info('Starting Sensum ETL job!')
        get_sensum_data()
        get_sensum_sager()
        return True

    except Exception as e:
        logger.error(f'An error occurred: {e}')
        return False
