import logging
from asset.asset_data import (
    insert_departments_data, insert_users_data, insert_computers_data, insert_device_license_and_historical_data
)

logger = logging.getLogger(__name__)


def job():
    try:
        logger.info("Starting Asset data job")

        insert_departments_data()

        insert_users_data()

        insert_computers_data()

        insert_device_license_and_historical_data()

        logger.info("Asset job completed successfully.")
        return True

    except Exception as e:
        logger.error(f"Error in Asset job execution: {e}")
        return False
