import logging
import requests
logger = logging.getLogger(__name__)

def job():
    logging.info("Test job")
    logger.info(requests.get("https://api.statbank.dk/v1/subjects"))
    return True
