import logging
from utils.database_connection import get_asset_db, get_capa_cms_db
from capa.capa_data import (
    create_capa_table_if_not_exists, get_serial_number, insert_serial_numbers,
    get_primary_user, update_primary_user, get_department, update_department,
    get_fullname, update_fullname, update_device_license_for_computers
)
from utils.config import ASSET_SFTP_FILE_PATH

capa_cms_db_client = get_capa_cms_db()
asset_db_client = get_asset_db()

logger = logging.getLogger(__name__)


def job():
    try:
        logger.info("Starting Capa data job")

        create_capa_table_if_not_exists(asset_db_client)

        serial_number_result = get_serial_number(capa_cms_db_client)
        if serial_number_result:
            insert_serial_numbers(asset_db_client, serial_number_result)
        else:
            logger.info("No serial numbers found.")

        primary_user_result = get_primary_user(capa_cms_db_client)
        if primary_user_result:
            update_primary_user(asset_db_client, primary_user_result)
        else:
            logger.info("No primary user data found.")

        department_result = get_department(capa_cms_db_client)
        if department_result:
            update_department(asset_db_client, department_result)
        else:
            logger.info("No department data found.")

        fullname_result = get_fullname(capa_cms_db_client)
        if fullname_result:
            update_fullname(asset_db_client, fullname_result)
        else:
            logger.info("No Fullname data found")

        update_device_license_for_computers(asset_db_client, ASSET_SFTP_FILE_PATH)

        return True

    except Exception as e:
        logger.error(f"Error in job execution: {e}")
        return False
