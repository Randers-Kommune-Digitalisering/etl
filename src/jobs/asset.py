import logging
from utils.database_connection import get_asset_db, get_capa_cms_db
from asset.asset_data import (
    get_serial_number, insert_serial_numbers,
    get_primary_user, update_primary_user, get_department, update_department,
    get_fullname, update_fullname, update_device_license_for_computers, update_price_from_atea,
    get_device_type, update_device_type, get_producent, update_producent, get_os, update_os, get_last_online, update_last_online,
    get_last_install_date, update_last_install_date, get_mac_addresses, update_mac_addresses, get_bitlocker_code, update_bitlocker_code,
    get_bitlocker_encryption, update_bitlocker_encryption, get_bitlocker_status, update_bitlocker_status, get_model, update_model,
    update_historical_data_from_comm2ig, update_ean_from_atea, update_afdelings_ean_from_delta

)
from utils.config import ASSET_SFTP_DEVICE_FILE_PATH, ASSET_SFTP_COMM2IG_HISTORICAL_FILE_PATH, ASSET_SFTP_EAN_ATEA_FILE_PATH, ASSET_SFTP_AFDELINGS_EAN_DELTA_FILE_PATH

capa_cms_db_client = get_capa_cms_db()
asset_db_client = get_asset_db()

logger = logging.getLogger(__name__)


def job():
    try:
        logger.info("Starting Asset data job")

        serial_number_result = get_serial_number(capa_cms_db_client)
        if serial_number_result:
            insert_serial_numbers(asset_db_client, serial_number_result)
        else:
            logger.info("No serial numbers found.")

        producent_result = get_producent(capa_cms_db_client)
        if producent_result:
            update_producent(asset_db_client, producent_result)
        else:
            logger.info("No producent data found.")

        device_type_result = get_device_type(capa_cms_db_client)
        if device_type_result:
            update_device_type(asset_db_client, device_type_result)
        else:
            logger.info("No device type data found.")

        os_deployment_result = get_os(capa_cms_db_client)
        if os_deployment_result:
            update_os(asset_db_client, os_deployment_result)
        else:
            logger.info("No OS data found.")

        last_online_result = get_last_online(capa_cms_db_client)
        if last_online_result:
            update_last_online(asset_db_client, last_online_result)
        else:
            logger.info("No last online data found.")

        primary_user_result = get_primary_user(capa_cms_db_client)
        if primary_user_result:
            update_primary_user(asset_db_client, primary_user_result)
        else:
            logger.info("No primary user data found.")

        last_install_dateresult = get_last_install_date(capa_cms_db_client)
        if last_install_dateresult:
            update_last_install_date(asset_db_client, last_install_dateresult)
        else:
            logger.info("No last install date data found.")

        mac_addresses_result = get_mac_addresses(capa_cms_db_client)
        if mac_addresses_result:
            update_mac_addresses(asset_db_client, mac_addresses_result)
        else:
            logger.info("No MAC addresses data found.")

        department_result = get_department(capa_cms_db_client)
        if department_result:
            update_department(asset_db_client, department_result)
        else:
            logger.info("No department data found.")

        bitlocker_code_result = get_bitlocker_code(capa_cms_db_client)
        if bitlocker_code_result:
            update_bitlocker_code(asset_db_client, bitlocker_code_result)
        else:
            logger.info("No BitLocker code data found.")

        bitlocker_encryption_result = get_bitlocker_encryption(capa_cms_db_client)
        if bitlocker_encryption_result:
            update_bitlocker_encryption(asset_db_client, bitlocker_encryption_result)
        else:
            logger.info("No BitLocker Encryption data found.")

        bitlocker_status_result = get_bitlocker_status(capa_cms_db_client)
        if bitlocker_status_result:
            update_bitlocker_status(asset_db_client, bitlocker_status_result)
        else:
            logger.info("No BitLocker status data found.")

        model_result = get_model(capa_cms_db_client)
        if model_result:
            update_model(asset_db_client, model_result)
        else:
            logger.info("No Model data found")

        fullname_result = get_fullname(capa_cms_db_client)
        if fullname_result:
            update_fullname(asset_db_client, fullname_result)
        else:
            logger.info("No Fullname data found")

        update_device_license_for_computers(asset_db_client, ASSET_SFTP_DEVICE_FILE_PATH)

        update_price_from_atea(asset_db_client)

        update_historical_data_from_comm2ig(asset_db_client, ASSET_SFTP_COMM2IG_HISTORICAL_FILE_PATH)

        update_ean_from_atea(asset_db_client, ASSET_SFTP_EAN_ATEA_FILE_PATH)

        update_afdelings_ean_from_delta(asset_db_client, ASSET_SFTP_AFDELINGS_EAN_DELTA_FILE_PATH)

        return True

    except Exception as e:
        logger.error(f"Error in Asset job execution: {e}")
        return False
