import logging
from utils.sftp_connection import get_asset_sftp_client
from io import StringIO
import pandas as pd

logger = logging.getLogger(__name__)


def create_capa_table_if_not_exists(db_client):
    check_table_sql = """
    CREATE TABLE IF NOT EXISTS Capa (
        UnitName VARCHAR(255) PRIMARY KEY,
        Serienummer VARCHAR(255),
        PrimaryUser VARCHAR(255),
        Afdeling VARCHAR(255),
        PrimaryFullName VARCHAR(255),
        DeviceLicense VARCHAR(255)
    );
    """
    try:
        db_client.execute_sql(check_table_sql)
        logger.info("Checked and created Capa table if not exists.")
    except Exception as e:
        logger.error(f"Error creating Capa table: {e}")


def get_serial_number(db_client):
    sql_command = """
    SELECT UNIT.NAME, UNIT.SERIALNUMBER
    FROM UNIT
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = db_client.execute_sql(sql_command)
        logger.info(f"SQL result: {result}")
        if result:
            filtered_result = []
            for row in result:
                unit_name, serial_number = row
                if serial_number:
                    logger.info(f"Unit Name: {unit_name}, Serial Number: {serial_number}")
                    filtered_result.append((unit_name, serial_number))
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return None
    except Exception as e:
        logger.error(f"Error retrieving capa data: {e}")
        return None


def insert_serial_numbers(db_client, data):
    check_sql_command = """
    SELECT COUNT(*)
    FROM Capa
    WHERE UnitName = :unit_name
    """
    insert_sql_command = """
    INSERT INTO Capa (UnitName, Serienummer)
    VALUES (:unit_name, :serial_number)
    """
    try:
        for row in data:
            unit_name, serial_number = row
            result = db_client.execute_sql(check_sql_command, {'unit_name': unit_name})
            if result and result[0][0] == 0:
                db_client.execute_sql(insert_sql_command, {'unit_name': unit_name, 'serial_number': serial_number})
        logger.info("Data inserted successfully into Capa table.")
    except Exception as e:
        logger.error(f"Error inserting data into Capa table: {e}")


def get_primary_user(db_client):
    sql_command = """
    SELECT UNIT.NAME, REPLACE(LGI.VALUE, '@RANDERS.DK', '') AS USER_NAME
    FROM UNIT
    JOIN LGI ON UNIT.UNITID = LGI.UNITID
    WHERE LGI.SECTION = 'Current Logon' AND LGI.NAME = 'User Name'
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = db_client.execute_sql(sql_command)
        logger.info(f"SQL result: {result}")
        if result:
            filtered_result = []
            for row in result:
                unit_name, user_name = row
                if not (unit_name.startswith('DQ') or unit_name.startswith('AP')):
                    logger.info(f"Unit Name: {unit_name}, Primary User: {user_name}")
                    filtered_result.append((unit_name, user_name))
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving primary user data: {e}")
        return None


def update_primary_user(db_client, data):
    sql_command = """
    UPDATE Capa
    SET PrimaryUser = :user_name
    WHERE UnitName = :unit_name
    """
    try:
        for row in data:
            unit_name, user_name = row
            db_client.execute_sql(sql_command, {'user_name': user_name, 'unit_name': unit_name})
        logger.info("Primary User Data updated successfully in Capa table.")
    except Exception as e:
        logger.error(f"Error updating data in Capa table: {e}")


def get_department(db_client):
    sql_command = """
    WITH PrimaryUsers AS (
        SELECT UNIT.UNITID, UNIT.NAME AS PC_UNIT_NAME, LGI.VALUE , REPLACE(LGI.VALUE, '@RANDERS.DK', '')  AS PRIMARY_USER
        FROM UNIT
        JOIN LGI ON UNIT.UNITID = LGI.UNITID
        WHERE LGI.SECTION = 'Current Logon' AND LGI.NAME = 'User Name'
    )
    SELECT DU.PC_UNIT_NAME, USI.VALUE AS DEPARTMENT
    FROM PrimaryUsers DU
    JOIN UNIT ON UNIT.NAME = DU.PRIMARY_USER
    JOIN USI ON UNIT.UNITID = USI.UNITID
    WHERE USI.SECTION = 'General User Inventory' AND USI.NAME = 'Department'
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = db_client.execute_sql(sql_command)
        logger.info(f"SQL result: {result}")
        if result:
            filtered_result = []
            for row in result:
                if len(row) == 2:
                    pc_unit_name, department = row
                    if not (pc_unit_name.startswith('DQ') or pc_unit_name.startswith('AP')):
                        logger.info(f"PC Unit Name: {pc_unit_name}, Department: {department}")
                        filtered_result.append((pc_unit_name, department))
                else:
                    logger.error(f"Unexpected row format: {row}")
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving department data: {e}")
        return None


def update_department(db_client, data):
    sql_command = """
    UPDATE Capa
    SET Afdeling = :department
    WHERE UnitName = :unit_name
    """
    try:
        for row in data:
            if len(row) == 2:
                pc_unit_name, department = row
                db_client.execute_sql(sql_command, {'department': department, 'unit_name': pc_unit_name})
            else:
                logger.error(f"Unexpected row format: {row}")
        logger.info("Department Data updated successfully in Capa table.")
    except Exception as e:
        logger.error(f"Error updating data in Capa table: {e}")


def get_fullname(db_client):
    sql_command = """
    WITH PrimaryUsers AS (
        SELECT UNIT.UNITID, UNIT.NAME AS PC_UNIT_NAME, LGI.VALUE, REPLACE(LGI.VALUE, '@RANDERS.DK', '') AS PRIMARY_USER
        FROM UNIT
        JOIN LGI ON UNIT.UNITID = LGI.UNITID
        WHERE LGI.SECTION = 'Current Logon' AND LGI.NAME = 'User Name'
    )
    SELECT DU.PC_UNIT_NAME, USI.VALUE AS FULLNAME
    FROM PrimaryUsers DU
    JOIN UNIT ON UNIT.NAME = DU.PRIMARY_USER
    JOIN USI ON UNIT.UNITID = USI.UNITID
    WHERE USI.SECTION = 'General User Inventory' AND USI.NAME = 'Full Name'
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = db_client.execute_sql(sql_command)
        if result:
            filtered_result = []
            for row in result:
                if len(row) == 2:
                    pc_unit_name, fullname = row
                    if not (pc_unit_name.startswith('DQ') or pc_unit_name.startswith('AP')):
                        logger.info(f"PC Unit Name: {pc_unit_name}, Full Name: {fullname}")
                        filtered_result.append((pc_unit_name, fullname))
                else:
                    logger.error(f"Unexpected row format: {row}")
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving full name data: {e}")
        return None


def update_fullname(db_client, data):
    sql_command = """
    UPDATE Capa
    SET PrimaryFullName = :fullname
    WHERE UnitName = :unit_name
    """
    try:
        for row in data:
            if len(row) == 2:
                pc_unit_name, fullname = row
                db_client.execute_sql(sql_command, {'fullname': fullname, 'unit_name': pc_unit_name})
                logger.info(f"Updated Unit Name: {pc_unit_name} with Full Name: {fullname}")
            else:
                logger.error(f"Unexpected row format: {row}")
        logger.info("Full Name Data updated successfully in Capa table.")
    except Exception as e:
        logger.error(f"Error updating data in Capa table: {e}")


def update_device_license(db_client, unit_name):
    try:
        sql_command = """
        UPDATE Capa
        SET DeviceLicense = :device_license
        WHERE UnitName = :unit_name
        """
        db_client.execute_sql(sql_command, {'device_license': 'TRUE', 'unit_name': unit_name})
        logger.info(f"Updated Device License for Unit Name: {unit_name}")
    except Exception as e:
        logger.error(f"Error updating device license for {unit_name}: {e}")


def get_all_unit_names(db_client):
    try:
        sql_command = "SELECT UnitName FROM Capa"
        result = db_client.execute_sql(sql_command)
        unit_names = [row[0] for row in result]
        return unit_names
    except Exception as e:
        logger.error(f"Error retrieving unit names: {e}")
        return []


def update_device_license_for_computers(db_client, sftp_file_path):
    try:
        csv_data = download_csv_from_asset_sftp(sftp_file_path)
        computer_names = get_computer_names_from_csv(csv_data)
        all_unit_names = get_all_unit_names(db_client)

        if all_unit_names:
            for computer_name in computer_names:
                if computer_name in all_unit_names:
                    update_device_license(db_client, computer_name)
                else:
                    logger.info(f"No matching Unit Name found for Computer Name: {computer_name}")
        else:
            logger.info("No unit names found in ComputerAssets table.")
    except Exception as e:
        logger.error(f"Error updating data  : {e}")
        return False


def get_computer_names_from_csv(csv_data):
    df = pd.read_csv(StringIO(csv_data))
    computer_names = [name for name in df['ComputerName'].tolist() if name]
    logger.info(f"Computer Names With Device License: {computer_names}")
    return computer_names


def download_csv_from_asset_sftp(sftp_file_path):
    sftp_client = get_asset_sftp_client()
    with sftp_client.get_connection() as conn:
        with conn.open(sftp_file_path, 'r') as file:
            csv_data = file.read().decode('utf-8')
    return csv_data
