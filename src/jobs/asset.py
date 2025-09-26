import logging
from utils.database_connection import get_asset_db, get_capa_cms_db
from asset.asset_data import (
    get_serial_number, insert_serial_numbers,
    get_primary_user, update_primary_user, get_department, update_department,
    get_fullname, update_fullname, update_device_license_for_computers, update_price_from_atea,
    get_device_type, update_device_type, get_producent, update_producent, get_os, update_os, get_last_online, update_last_online,
    get_last_install_date, update_last_install_date, get_mac_addresses, update_mac_addresses, get_bitlocker_code, update_bitlocker_code,
    get_bitlocker_encryption, update_bitlocker_encryption, get_bitlocker_status, update_bitlocker_status, get_model, update_model,
    update_historical_data_from_comm2ig, update_ean_from_atea, update_afdelings_ean_from_delta, upload_assets_to_topdesk

)
from utils.config import (
    ASSET_SFTP_DEVICE_FILE_PATH,
    ASSET_SFTP_COMM2IG_HISTORICAL_FILE_PATH,
    ASSET_SFTP_EAN_ATEA_FILE_PATH,
    ASSET_SFTP_AFDELINGS_EAN_DELTA_FILE_PATH
)

capa_cms_db_client = get_capa_cms_db()
asset_db_client = get_asset_db()

logger = logging.getLogger(__name__)


def job():
    try:
        logger.info("Starting Asset data job")

        serial_number_result = get_serial_number(capa_cms_db_client)
        if serial_number_result:
            if not insert_serial_numbers(asset_db_client, serial_number_result):
                logger.error("Failed to insert serial numbers.")
                return False
        else:
            logger.info("No serial numbers found.")

        producent_result = get_producent(capa_cms_db_client)
        if producent_result:
            if not update_producent(asset_db_client, producent_result):
                logger.error("Failed to update producent.")
                return False
        else:
            logger.info("No producent data found.")

        device_type_result = get_device_type(capa_cms_db_client)
        if device_type_result:
            if not update_device_type(asset_db_client, device_type_result):
                logger.error("Failed to update device type.")
                return False
        else:
            logger.info("No device type data found.")

        os_deployment_result = get_os(capa_cms_db_client)
        if os_deployment_result:
            if not update_os(asset_db_client, os_deployment_result):
                logger.error("Failed to update OS.")
                return False
        else:
            logger.info("No OS data found.")

        last_online_result = get_last_online(capa_cms_db_client)
        if last_online_result:
            if not update_last_online(asset_db_client, last_online_result):
                logger.error("Failed to update last online.")
                return False
        else:
            logger.info("No last online data found.")

        primary_user_result = get_primary_user(capa_cms_db_client)
        if primary_user_result:
            if not update_primary_user(asset_db_client, primary_user_result):
                logger.error("Failed to update primary user.")
                return False
        else:
            logger.info("No primary user data found.")

        last_install_dateresult = get_last_install_date(capa_cms_db_client)
        if last_install_dateresult:
            if not update_last_install_date(asset_db_client, last_install_dateresult):
                logger.error("Failed to update last install date.")
                return False
        else:
            logger.info("No last install date data found.")

        mac_addresses_result = get_mac_addresses(capa_cms_db_client)
        if mac_addresses_result:
            if not update_mac_addresses(asset_db_client, mac_addresses_result):
                logger.error("Failed to update MAC addresses.")
                return False
        else:
            logger.info("No MAC addresses data found.")

        department_result = get_department(capa_cms_db_client)
        if department_result:
            if not update_department(asset_db_client, department_result):
                logger.error("Failed to update department.")
                return False
        else:
            logger.info("No department data found.")

        bitlocker_code_result = get_bitlocker_code(capa_cms_db_client)
        if bitlocker_code_result:
            if not update_bitlocker_code(asset_db_client, bitlocker_code_result):
                logger.error("Failed to update BitLocker code.")
                return False
        else:
            logger.info("No BitLocker code data found.")

        bitlocker_encryption_result = get_bitlocker_encryption(capa_cms_db_client)
        if bitlocker_encryption_result:
            if not update_bitlocker_encryption(asset_db_client, bitlocker_encryption_result):
                logger.error("Failed to update BitLocker encryption.")
                return False
        else:
            logger.info("No BitLocker Encryption data found.")

        bitlocker_status_result = get_bitlocker_status(capa_cms_db_client)
        if bitlocker_status_result:
            if not update_bitlocker_status(asset_db_client, bitlocker_status_result):
                logger.error("Failed to update BitLocker status.")
                return False
        else:
            logger.info("No BitLocker status data found.")

        model_result = get_model(capa_cms_db_client)
        if model_result:
            if not update_model(asset_db_client, model_result):
                logger.error("Failed to update model.")
                return False
        else:
            logger.info("No Model data found")

        fullname_result = get_fullname(capa_cms_db_client)
        if fullname_result:
            if not update_fullname(asset_db_client, fullname_result):
                logger.error("Failed to update fullname.")
                return False
        else:
            logger.info("No Fullname data found")

        if not update_device_license_for_computers(asset_db_client, ASSET_SFTP_DEVICE_FILE_PATH):
            logger.error("Failed to update device license for computers.")
            return False

        if not update_price_from_atea(asset_db_client):
            logger.error("Failed to update price from Atea.")
            return False

        if not update_historical_data_from_comm2ig(asset_db_client, ASSET_SFTP_COMM2IG_HISTORICAL_FILE_PATH):
            logger.error("Failed to update historical data from Comm2IG.")
            return False

        if not update_ean_from_atea(asset_db_client, ASSET_SFTP_EAN_ATEA_FILE_PATH):
            logger.error("Failed to update EAN from Atea.")
            return False

        if not update_afdelings_ean_from_delta(asset_db_client, ASSET_SFTP_AFDELINGS_EAN_DELTA_FILE_PATH):
            logger.error("Failed to update afdelings EAN from Delta.")
            return False

        if not upload_assets_to_topdesk(asset_db_client):
            logger.error("Failed to upload assets to Topdesk.")
            return False

        logger.info("Asset job completed successfully.")
        return True

    except Exception as e:
        logger.error(f"Error in Asset job execution: {e}")
        return False
