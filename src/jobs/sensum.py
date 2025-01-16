from sensum.sensum_data import get_sensum_data
from sensum.sensum_sager import get_sensum_sager
from sensum.sensum_sags_aktiviteter import get_sensum_sags_aktiviteter
from sensum.sensum_medarbejder import get_sensum_medarbejder
import logging
logger = logging.getLogger(__name__)


def job():
    try:
        logger.info('Starting Sensum ETL job!')
        get_sensum_data()
        get_sensum_sager()
        get_sensum_sags_aktiviteter()
        get_sensum_medarbejder()
        return True

    except Exception as e:
        logger.error(f'An error occurred: {e}')
        return False
