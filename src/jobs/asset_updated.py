import logging
from asset.asset_data_updated import (
    create_asset_tables, insert_departments_data, insert_users_data, insert_computers_data
)

logger = logging.getLogger(__name__)


def job():
    try:
        logger.info("Starting Asset data job")

        create_asset_tables()

        insert_departments_data()

        insert_users_data()

        insert_computers_data()

        logger.info("Asset job completed successfully.")
        return True

    except Exception as e:
        logger.error(f"Error in Asset job execution: {e}")
        return False
