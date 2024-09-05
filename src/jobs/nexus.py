
import logging

from custom_data_connector import read_data_from_custom_data_connector

logger = logging.getLogger(__name__)


def job():
    logger.info("Initializing Nexus job")

    # Read data from custom data connector
    filename = 'DATA_BIL54.csv'

    file = read_data_from_custom_data_connector(filename)

    logger.info(file)
