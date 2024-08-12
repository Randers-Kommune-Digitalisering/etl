import logging
import requests
logger = logging.getLogger(__name__)

def job():
    logging.info("Test job")
    try:
        res  = requests.get("https://api.statbank.dk/v1/subjects")
        logger.info(res)
    except Exception as e:
        logger.error(e)
        return False
    return True
