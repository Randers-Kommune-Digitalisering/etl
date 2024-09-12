from jobindsats.jobindsats_y30r21 import get_jobindats_ydelsesgrupper
from jobindsats.jobindsats_y01a02 import get_jobindsats_dagpenge
import logging

logger = logging.getLogger(__name__)


def job():
    logger.info('Starting jobindsats ETL jobs')
    get_jobindsats_dagpenge()
    get_jobindats_ydelsesgrupper()
    return True
