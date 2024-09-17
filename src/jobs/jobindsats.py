from jobindsats.jobindsats_y30r21 import get_jobindats_ydelsesgrupper
from jobindsats.jobindsats_y01a02 import get_jobindsats_dagpenge
from jobindsats.jobindsats_y07a02 import get_jobindsats_syg_dagpenge
from jobindsats.jobindsats_y08a02 import get_jobindsats_fleksjob
from jobindsats.jobindsats_y09a02 import get_jobindsats_ledighedsydelse
from jobindsats.jobindsats_y12a02 import get_jobindsats_jobafklaringsforløb
from jobindsats.jobindsats_y35a02 import get_jobindsats_sho
from jobindsats.jobindsats_y36a02 import get_jobindsats_kontanthjælp
from jobindsats.jobindsats_y38a02 import get_jobindsats_uddannelseshjælp
from jobindsats.jobindsats_y11a02 import get_jobindsats_ressourceforløb
from jobindsats.jobindsats_y04a02 import get_jobindsats_revalidering
from jobindsats.jobindsats_y14d03 import get_jobindsats_ydelse_til_job
from jobindsats.jobindsats_y10a02 import get_jobindsats_tilbagetraekningsydelser
from jobindsats.jobindsats_otij01 import get_jobindsats_ydelsesmodtagere_loentimer
from jobindsats.jobindsats_ptvc01 import get_jobindsats_tilbud_samtaler
from jobindsats.jobindsats_ptva02 import get_jobindsats_alle_ydelser

import logging

logger = logging.getLogger(__name__)


def job():
    try:
        logger.info('Starting jobindsats ETL jobs!')
        get_jobindsats_alle_ydelser()
        get_jobindsats_tilbud_samtaler()
        get_jobindsats_ydelsesmodtagere_loentimer()
        get_jobindsats_tilbagetraekningsydelser()
        get_jobindsats_ydelse_til_job()
        get_jobindsats_revalidering()
        get_jobindsats_ressourceforløb()
        get_jobindsats_uddannelseshjælp()
        get_jobindsats_kontanthjælp()
        get_jobindsats_sho()
        get_jobindsats_jobafklaringsforløb()
        get_jobindsats_ledighedsydelse()
        get_jobindsats_fleksjob()
        get_jobindsats_syg_dagpenge()
        get_jobindsats_dagpenge()
        get_jobindats_ydelsesgrupper()
        return True
    except Exception as e:
        logger.error(f'An error occurred: {e}')
        return False
