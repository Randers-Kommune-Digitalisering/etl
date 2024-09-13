from jobindsats.jobindsats_y30r21 import get_jobindats_ydelsesgrupper
from jobindsats.jobindsats_y01a02 import get_jobindsats_dagpenge
from jobindsats.jobindsats_y07a02 import get_jobindsats_syg_dagpenge
from jobindsats.jobindsats_y08a02 import get_jobindsats_fleksjob

import logging

logger = logging.getLogger(__name__)


def job():
    try:
        logger.info('Starting jobindsats ETL jobs!')
        get_jobindsats_fleksjob()
        get_jobindsats_syg_dagpenge()
        get_jobindsats_dagpenge()
        get_jobindats_ydelsesgrupper()
        return True
    except Exception as e:
        logger.error(f'An error occurred: {e}')
        return False
