import logging
from asset.asset_data import (
    create_asset_tables, insert_departments_data, insert_users_data, insert_computers_data,
    insert_device_license_and_historical_data, insert_atea_data, upload_assets_to_topdesk
)

logger = logging.getLogger(__name__)


def job():
    try:
        logger.info("Starting Asset data job")

        create_asset_tables()

        if not insert_departments_data():
            logger.error("Failed to insert departments data.")
            return False

        if not insert_users_data():
            logger.error("Failed to insert users data.")
            return False

        if not insert_computers_data():
            logger.error("Failed to insert computers data.")
            return False

        if not insert_device_license_and_historical_data():
            logger.error("Failed to insert device license and historical data.")
            return False

        if not insert_atea_data():
            logger.error("Failed to insert Atea data.")
            return False

        if not upload_assets_to_topdesk():
            logger.error("Failed to upload assets to TopDesk.")
            return False

        logger.info("Asset job completed successfully.")
        return True

    except Exception as e:
        logger.error(f"Error in Asset job execution: {e}")
        return False
