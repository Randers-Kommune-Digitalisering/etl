import logging
from utils.database_client import DatabaseClient
from utils.config import (
    ASSET_MANAGEMENT_LIGHT_DB_HOST,
    ASSET_MANAGEMENT_LIGHT_DB_USER,
    ASSET_MANAGEMENT_LIGHT_DB_PASS,
    ASSET_MANAGEMENT_LIGHT_DB_DATABASE,
    SSHW_DB_DATABASE,
    SSHW_DB_USER,
    SSHW_DB_PASS,
    SSHW_DB_HOST
)

capa_db_client = DatabaseClient(
    database=ASSET_MANAGEMENT_LIGHT_DB_DATABASE,
    username=ASSET_MANAGEMENT_LIGHT_DB_USER,
    password=ASSET_MANAGEMENT_LIGHT_DB_PASS,
    host=ASSET_MANAGEMENT_LIGHT_DB_HOST
)

sshw_db_client = DatabaseClient(
    database=SSHW_DB_DATABASE,
    username=SSHW_DB_USER,
    password=SSHW_DB_PASS,
    host=SSHW_DB_HOST
)

logger = logging.getLogger(__name__)


def job():
    try:
        logger.info("Starting Asset-Management-Light job")

        # Get serial numbers and Unitname from CAPA DB and insert to new DB
        serial_number_result = get_serial_number(capa_db_client)
        if serial_number_result:
            for row in serial_number_result:
                logger.info(f"Serial Number: {row}")
            insert_serial_numbers(sshw_db_client, serial_number_result)
        else:
            logger.info("No serial numbers found.")

        return True
    except Exception as e:
        logger.error(f"Error in Asset-Management-Light job: {e}")
        return False


def get_serial_number(capa_db_client):
    sql_command = """
    SELECT UNIT.NAME, UNIT.SERIALNUMBER
    FROM UNIT
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_db_client.execute_sql(sql_command)
        if result:
            filtered_result = []
            for row in result:
                unit_name, serial_number = row
                if serial_number:
                    logger.info(f"Unit Name: {unit_name}, Serial Number: {serial_number}")
                    filtered_result.append(row)
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        capa_db_client.logger.error(f"Error retrieving capa data: {e}")
        return None


def insert_serial_numbers(sshw_db_client, data):
    check_sql_command = """
    SELECT COUNT(*)
    FROM ComputerAssets
    WHERE UnitName = %s
    """
    insert_sql_command = """
    INSERT INTO ComputerAssets (UnitName, Serienummer)
    VALUES (%s, %s)
    """
    try:
        for row in data:
            unit_name, serial_number = row
            result = sshw_db_client.execute_sql(check_sql_command, (unit_name,))
            if result[0][0] == 0:
                sshw_db_client.execute_sql(insert_sql_command, (unit_name, serial_number))
        sshw_db_client.get_connection().commit()
        logger.info("Data inserted successfully into ComputerAssets table.")
    except Exception as e:
        logger.error(f"Error inserting data into ComputerAssets table: {e}")
