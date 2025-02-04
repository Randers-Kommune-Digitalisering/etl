import logging
from utils.database_client import DatabaseClient
from utils.config import (
    CAPA_DB_HOST,
    CAPA_DB_USER,
    CAPA_DB_PASS,
    CAPA_DB_DATABASE,
    SSHW_DB_DATABASE,
    SSHW_DB_HOST,
    SSHW_DB_PASS,
    SSHW_DB_USER,
)

capa_db_client = DatabaseClient(
    database=CAPA_DB_DATABASE,
    username=CAPA_DB_USER,
    password=CAPA_DB_PASS,
    host=CAPA_DB_HOST,
    db_type='mssql'
)

sshw_db_client = DatabaseClient(
    database=SSHW_DB_DATABASE,
    username=SSHW_DB_USER,
    password=SSHW_DB_PASS,
    host=SSHW_DB_HOST,
    db_type='mysql'
)

logger = logging.getLogger(__name__)


def job():
    try:
        logger.info("Starting Asset-Management-Light job")

        # Create ComputerAssets table if not exists
        create_computer_assets_table_if_not_exists(sshw_db_client)

        # Get serial numbers and Unitname from CAPA DB and insert to new DB
        serial_number_result = get_serial_number(capa_db_client)
        if serial_number_result:
            for row in serial_number_result:
                logger.info(f"Serial Number: {row}")
            insert_serial_numbers(sshw_db_client, serial_number_result)
        else:
            logger.info("No serial numbers found.")

        # # Insert/Update Producent
        producent_result = get_producent(capa_db_client)
        if producent_result:
            for row in producent_result:
                logger.info(f"Producent: {row}")
            update_producent(sshw_db_client, producent_result)
        else:
            logger.info("No producent data found.")

        # # Insert/Update Device Type
        device_type_result = get_device_type(capa_db_client)
        if device_type_result:
            for row in device_type_result:
                logger.info(f"Device Type: {row}")
            update_device_type(sshw_db_client, device_type_result)
        else:
            logger.info("No device type data found.")

        # # Insert/Update OS
        os_deployment_result = get_os(capa_db_client)
        if os_deployment_result:
            for row in os_deployment_result:
                logger.info(f"OS: {row}")
            update_os(sshw_db_client, os_deployment_result)
        else:
            logger.info("No OS data found.")

        # # Insert/Update Last Online
        last_online_result = get_last_online(capa_db_client)
        if last_online_result:
            for row in last_online_result:
                logger.info(f"Last Online: {row}")
            update_last_online(sshw_db_client, last_online_result)
        else:
            logger.info("No last online data found.")

        # # Insert/Update Primary User
        primary_user_result = get_primary_user(capa_db_client)
        if primary_user_result:
            for row in primary_user_result:
                logger.info(f"primary User: {row}")
            update_primary_user(sshw_db_client, primary_user_result)
        else:
            logger.info("No primary user data found.")

        # # Insert/Update Last Install Date
        last_install_dateresult = get_last_install_date(capa_db_client)
        if last_install_dateresult:
            for row in last_install_dateresult:
                logger.info(f"Last Install Date: {row}")
            update_last_install_date(sshw_db_client, last_install_dateresult)
        else:
            logger.info("No last install date data found.")

        # # Insert/Update MAC Addresses
        mac_addresses_result = get_mac_addresses(capa_db_client)
        if mac_addresses_result:
            for row in mac_addresses_result:
                logger.info(f"MAC Addresses: {row}")
            update_mac_addresses(sshw_db_client, mac_addresses_result)
        else:
            logger.info("No MAC addresses data found.")

        # Insert/Update Department
        department_result = get_department(capa_db_client)
        if department_result:
            for row in department_result:
                logger.info(f"Department: {row}")
            update_department(sshw_db_client, department_result)
        else:
            logger.info("No department data found.")

        # Insert/Update BitLocker Code
        bitlocker_code_result = get_bitlocker_code(capa_db_client)
        if bitlocker_code_result:
            for row in bitlocker_code_result:
                logger.info(f"BitLocker Code: {row}")
            update_bitlocker_code(sshw_db_client, bitlocker_code_result)
        else:
            logger.info("No BitLocker code data found.")

        # Insert/Update BitLocker Encryption
        bitlocker_encryption_result = get_bitlocker_encryption(capa_db_client)
        if bitlocker_encryption_result:
            for row in bitlocker_encryption_result:
                logger.info(f"BitLocker Encryption: {row}")
            update_bitlocker_encryption(sshw_db_client, bitlocker_encryption_result)
        else:
            logger.info("No BitLocker Encryption data found.")

        # Insert/Update BitLocker Status
        bitlocker_status_result = get_bitlocker_status(capa_db_client)
        if bitlocker_status_result:
            for row in bitlocker_status_result:
                logger.info(f"BitLocker Status: {row}")
            update_bitlocker_status(sshw_db_client, bitlocker_status_result)
        else:
            logger.info("No BitLocker status data found.")

        # # Insert/Update Model
        model_result = get_model(capa_db_client)
        if model_result:
            for row in model_result:
                logger.info(f"Model Status: {row}")
            update_model(sshw_db_client, model_result)
        else:
            logger.info("No Model data found")

        # Insert/Update FullName
        fullname_result = get_fullname(capa_db_client)
        if fullname_result:
            for row in fullname_result:
                logger.info(f"FullName Status: {row}")
            update_fullname(sshw_db_client, fullname_result)
        else:
            logger.info("No Fullname data found")

        return True
    except Exception as e:
        logger.error(f"Error in Asset-Management-Light job: {e}")
        return False


def create_computer_assets_table_if_not_exists(sshw_db_client):
    check_table_sql = """
    CREATE TABLE IF NOT EXISTS ComputerAssets (
        UnitName VARCHAR(255) PRIMARY KEY,
        Producent VARCHAR(255),
        Model VARCHAR(255),
        Enhedstype VARCHAR(255),
        Serienummer VARCHAR(255),
        PrimaryFullName VARCHAR(255),
        PrimaryUser VARCHAR(255),
        Afdeling VARCHAR(255),
        SidsteLoginDato VARCHAR(255),
        SidsteRul VARCHAR(255),
        BitlockerKode VARCHAR(255),
        BitlockerStatus VARCHAR(255),
        BitlockerKrypteringProcent VARCHAR(255),
        OSVersion VARCHAR(255),
        MACAdresse VARCHAR(255),
        DeviceLicense VARCHAR(255),
        LaanePC BIT
    );
    """
    try:
        sshw_db_client.execute_sql(check_table_sql)
        sshw_db_client.get_connection().commit()
        logger.info("Checked and created ComputerAssets table if not exists.")
    except Exception as e:
        logger.error(f"Error creating ComputerAssets table: {e}")


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


def get_producent(capa_db_client):
    sql_command = """
    SELECT UNIT.NAME, INV.VALUE
    FROM UNIT
    JOIN INV ON UNIT.UNITID = INV.UNITID
    WHERE (INV.SECTION = 'System') AND (INV.NAME = 'Manufacturer')
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_db_client.execute_sql(sql_command)
        if result:
            filtered_result = []
            for row in result:
                unit_name, producent = row
                if producent and not (unit_name.startswith('DQ') or unit_name.startswith('AP')):  # Filter out USERS(DQ/AP) ONLY PC's
                    logger.info(f"Unit Name: {unit_name}, Producent: {producent}")
                    filtered_result.append(row)
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        capa_db_client.logger.error(f"Error retrieving producent data: {e}")
        return None


def update_producent(sshw_db_client, data):
    sql_command = """
    UPDATE ComputerAssets
    SET Producent = %s
    WHERE UnitName = %s
    """
    try:
        for row in data:
            unit_name, producent = row
            sshw_db_client.execute_sql(sql_command, (producent, unit_name))
        sshw_db_client.get_connection().commit()
        logger.info("Producent Data updated successfully in ComputerAssets table.")
    except Exception as e:
        logger.error(f"Error updating data in ComputerAssets table: {e}")


def get_device_type(capa_db_client):
    sql_command = """
    SELECT UNIT.NAME, DEVICETYPE.HWNAME
    FROM UNIT
    JOIN DEVICETYPE ON UNIT.DEVICETYPEID = DEVICETYPE.ID
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_db_client.execute_sql(sql_command)
        if result:
            filtered_result = []
            for row in result:
                unit_name, device_type = row
                if not (unit_name.startswith('DQ') or unit_name.startswith('AP')):
                    logger.info(f"Unit Name: {unit_name}, Device Type: {device_type}")
                    filtered_result.append(row)
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        capa_db_client.logger.error(f"Error retrieving device type data: {e}")
        return None


def update_device_type(sshw_db_client, data):
    sql_command = """
    UPDATE ComputerAssets
    SET Enhedstype = %s
    WHERE UnitName = %s
    """
    try:
        for row in data:
            unit_name, device_type = row
            sshw_db_client.execute_sql(sql_command, (device_type, unit_name))
        sshw_db_client.get_connection().commit()
        logger.info("Device Type Data updated successfully in ComputerAssets table.")
    except Exception as e:
        logger.error(f"Error updating data in ComputerAssets table: {e}")


def get_os(capa_db_client):
    sql_command = """
    SELECT UNIT.NAME, INV.VALUE
    FROM UNIT
    JOIN INV ON UNIT.UNITID = INV.UNITID
    WHERE INV.SECTION = 'Operating System' AND INV.NAME = 'System'
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_db_client.execute_sql(sql_command)
        if result:
            filtered_result = []
            for row in result:
                unit_name, os_value = row
                if not (unit_name.startswith('DQ') or unit_name.startswith('AP')):
                    logger.info(f"Unit Name: {unit_name}, OS Deployment: {os_value}")
                    filtered_result.append((unit_name, os_value))
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving OS deployment data: {e}")
        return None


def update_os(sshw_db_client, data):
    sql_command = """
    UPDATE ComputerAssets
    SET OSVersion = %s
    WHERE UnitName = %s
    """
    try:
        for row in data:
            unit_name, os_value = row
            sshw_db_client.execute_sql(sql_command, (os_value, unit_name))
        sshw_db_client.get_connection().commit()
        logger.info("OS Deployment Data updated successfully in ComputerAssets table.")
    except Exception as e:
        logger.error(f"Error updating data in ComputerAssets table: {e}")


def get_last_online(capa_db_client):
    sql_command = """
    SELECT UNIT.NAME,
           FORMAT(DATEADD(HOUR, 1, DATEADD(SECOND, TRY_CAST(UNIT.LASTONLINE AS BIGINT), '1970-01-01')), 'yyyy-MM-ddTHH:mm:ss.ff') AS LASTONLINE
    FROM UNIT
    WHERE UNIT.NAME NOT LIKE 'DQ%' AND UNIT.NAME NOT LIKE 'AP%'
      AND UNIT.LASTONLINE IS NOT NULL
      AND ISNUMERIC(UNIT.LASTONLINE) = 1
      AND TRY_CAST(UNIT.LASTONLINE AS BIGINT) BETWEEN 0 AND 253402300799
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_db_client.execute_sql(sql_command)
        if result:
            filtered_result = []
            for row in result:
                unit_name, last_online = row
                logger.info(f"Unit Name: {unit_name}, Last Online: {last_online}")
                filtered_result.append((unit_name, last_online))
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving last online data: {e}")
        return None


def update_last_online(sshw_db_client, data):
    sql_command = """
    UPDATE ComputerAssets
    SET SidsteLoginDato = %s
    WHERE UnitName = %s
    """
    try:
        for row in data:
            unit_name, last_online = row
            sshw_db_client.execute_sql(sql_command, (last_online, unit_name))
        sshw_db_client.get_connection().commit()
        logger.info("Last Online Data updated successfully in ComputerAssets table.")
    except Exception as e:
        logger.error(f"Error updating data in ComputerAssets table: {e}")


def get_primary_user(capa_db_client):
    sql_command = """
    SELECT UNIT.NAME, REPLACE(LGI.VALUE, '@RANDERS.DK', '') AS USER_NAME
    FROM UNIT
    JOIN LGI ON UNIT.UNITID = LGI.UNITID
    WHERE LGI.SECTION = 'Current Logon' AND LGI.NAME = 'User Name'

    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_db_client.execute_sql(sql_command)
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


def update_primary_user(sshw_db_client, data):
    sql_command = """
    UPDATE ComputerAssets
    SET PrimaryUser = %s
    WHERE UnitName = %s
    """
    try:
        for row in data:
            unit_name, user_name = row
            sshw_db_client.execute_sql(sql_command, (user_name, unit_name))
        sshw_db_client.get_connection().commit()
        logger.info("Primary User Data updated successfully in ComputerAssets table.")
    except Exception as e:
        logger.error(f"Error updating data in ComputerAssets table: {e}")


def get_last_install_date(capa_db_client):
    sql_command = """
    SELECT UNIT.NAME,
           FORMAT(DATEADD(SECOND, TRY_CAST(INV.VALUE AS BIGINT), '1970-01-01'), 'yyyy-MM-ddTHH:mm:ss.ff') AS LAST_INSTALL_DATE
    FROM UNIT
    JOIN INV ON UNIT.UNITID = INV.UNITID
    WHERE INV.SECTION = 'Operating System' AND INV.NAME = 'InstallDate'
      AND INV.VALUE IS NOT NULL
      AND ISNUMERIC(INV.VALUE) = 1
      AND TRY_CAST(INV.VALUE AS BIGINT) BETWEEN 0 AND 253402300799
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_db_client.execute_sql(sql_command)
        if result:
            filtered_result = []
            for row in result:
                unit_name, last_install_date = row
                if not (unit_name.startswith('DQ') or unit_name.startswith('AP')):
                    logger.info(f"Unit Name: {unit_name}, Last Install Date: {last_install_date}")
                    filtered_result.append((unit_name, last_install_date))
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving last install date data: {e}")
        return None


def update_last_install_date(sshw_db_client, data):
    sql_command = """
    UPDATE ComputerAssets
    SET SidsteRul = %s
    WHERE UnitName = %s
    """
    try:
        for row in data:
            unit_name, last_install_date = row
            sshw_db_client.execute_sql(sql_command, (last_install_date, unit_name))
        sshw_db_client.get_connection().commit()
        logger.info("Last Install Date Data updated successfully in ComputerAssets table.")
    except Exception as e:
        logger.error(f"Error updating data in ComputerAssets table: {e}")


def get_mac_addresses(capa_db_client):
    sql_command = """
    SELECT UNIT.NAME,
           STRING_AGG(INV.VALUE, ', ') AS MAC_ADDRESSES
    FROM UNIT
    JOIN INV ON UNIT.UNITID = INV.UNITID
    WHERE INV.SECTION = 'Network Adapter'
      AND (INV.NAME = 'Device #1 MAC Address'
           OR INV.NAME = 'Device #2 MAC Address'
           OR INV.NAME = 'Device #3 MAC Address'
           OR INV.NAME = 'Device #4 MAC Address'
           OR INV.NAME = 'Device #5 MAC Address'
           OR INV.NAME = 'Device #6 MAC Address'
           OR INV.NAME = 'Device #7 MAC Address'
           OR INV.NAME = 'Device #8 MAC Address'
           OR INV.NAME = 'Device #9 MAC Address'
           OR INV.NAME = 'Device #10 MAC Address'
           OR INV.NAME = 'Device #11 MAC Address'
           OR INV.NAME = 'Device #12 MAC Address'
           OR INV.NAME = 'Device #13 MAC Address'
           OR INV.NAME = 'Device #14 MAC Address'
           OR INV.NAME = 'Device #15 MAC Address'
           OR INV.NAME = 'Device #16 MAC Address')
    GROUP BY UNIT.NAME
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_db_client.execute_sql(sql_command)
        if result:
            filtered_result = []
            for row in result:

                unit_name, mac_addresses = row
                if not (unit_name.startswith('DQ') or unit_name.startswith('AP')):
                    logger.info(f"Unit Name: {unit_name}, MAC Addresses: {mac_addresses}")
                    filtered_result.append((unit_name, mac_addresses))
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving MAC addresses data: {e}")
        return None


def update_mac_addresses(sshw_db_client, data):
    sql_command = """
    UPDATE ComputerAssets
    SET MACAdresse = %s
    WHERE UnitName = %s
    """
    try:
        for row in data:
            unit_name, mac_addresses = row
            sshw_db_client.execute_sql(sql_command, (mac_addresses, unit_name))
        sshw_db_client.get_connection().commit()
        logger.info("MAC Addresses Data updated successfully in ComputerAssets table.")
    except Exception as e:
        logger.error(f"Error updating data in ComputerAssets table: {e}")


def get_department(capa_db_client):
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
        result = capa_db_client.execute_sql(sql_command)
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


def update_department(sshw_db_client, data):
    sql_command = """
    UPDATE ComputerAssets
    SET Afdeling = %s
    WHERE UnitName = %s
    """
    try:
        for row in data:
            if len(row) == 2:
                pc_unit_name, department = row
                sshw_db_client.execute_sql(sql_command, (department, pc_unit_name))
            else:
                logger.error(f"Unexpected row format: {row}")
        sshw_db_client.get_connection().commit()
        logger.info("Department Data updated successfully in ComputerAssets table.")
    except Exception as e:
        logger.error(f"Error updating data in ComputerAssets table: {e}")


def get_bitlocker_code(capa_db_client):
    sql_command = """
    SELECT UNIT.NAME, CSI.VALUE
    FROM UNIT
    JOIN CSI ON UNIT.UNITID = CSI.UNITID
    WHERE CSI.SECTION = 'CapaServices | CapaBitLocker'
      AND (CSI.NAME = 'Recovery Password C: #1 Password'
           OR CSI.NAME = 'Recovery Password D: #1 Password'
           OR CSI.NAME = 'Recovery Password E: #1 Password')
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_db_client.execute_sql(sql_command)
        if result:
            filtered_result = []
            for row in result:

                unit_name, bitlocker_code = row
                if not (unit_name.startswith('DQ') or unit_name.startswith('AP')):
                    logger.info(f"Unit Name: {unit_name}, BitLocker Code: {bitlocker_code}")
                    filtered_result.append((unit_name, bitlocker_code))
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving BitLocker code data: {e}")
        return None


def update_bitlocker_code(sshw_db_client, data):
    sql_command = """
    UPDATE ComputerAssets
    SET BitlockerKode = %s
    WHERE UnitName = %s
    """
    try:
        for row in data:
            if len(row) == 2:
                unit_name, bitlocker_code = row
                sshw_db_client.execute_sql(sql_command, (bitlocker_code, unit_name))
            else:
                logger.error(f"Unexpected row format: {row}")
        sshw_db_client.get_connection().commit()
        logger.info("BitLocker Code Data updated successfully in ComputerAssets table.")
    except Exception as e:
        logger.error(f"Error updating data in ComputerAssets table: {e}")


def get_bitlocker_encryption(capa_db_client):
    sql_command = """
    SELECT UNIT.NAME, CSI.VALUE
    FROM UNIT
    JOIN CSI ON UNIT.UNITID = CSI.UNITID
    WHERE CSI.SECTION = 'CapaServices | CapaBitLocker' AND CSI.NAME = 'Encryption Status C:'
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_db_client.execute_sql(sql_command)
        if result:
            filtered_result = []
            for row in result:
                unit_name, bitlocker_status = row
                if not (unit_name.startswith('DQ') or unit_name.startswith('AP')):
                    logger.info(f"Unit Name: {unit_name}, BitLocker Status: {bitlocker_status}")
                    filtered_result.append((unit_name, bitlocker_status))
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving BitLocker Encryption data: {e}")
        return None


def update_bitlocker_encryption(sshw_db_client, data):
    sql_command = """
    UPDATE ComputerAssets
    SET BitlockerKrypteringProcent = %s
    WHERE UnitName = %s
    """
    try:
        for row in data:
            unit_name, bitlocker_status = row
            sshw_db_client.execute_sql(sql_command, (bitlocker_status, unit_name))
        sshw_db_client.get_connection().commit()
        logger.info("BitLocker Encryption Data updated successfully in ComputerAssets table.")
    except Exception as e:
        logger.error(f"Error updating data in ComputerAssets table: {e}")


def get_bitlocker_status(capa_db_client):
    sql_command = """
    SELECT UNIT.NAME, CSI.VALUE
    FROM UNIT
    JOIN CSI ON UNIT.UNITID = CSI.UNITID
    WHERE CSI.SECTION = 'CapaServices | CapaBitLocker' AND CSI.NAME = 'Protection Status C:'
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_db_client.execute_sql(sql_command)
        if result:
            filtered_result = []
            for row in result:

                unit_name, bitlocker_status = row
                if not (unit_name.startswith('DQ') or unit_name.startswith('AP')):
                    logger.info(f"Unit Name: {unit_name}, BitLocker Status: {bitlocker_status}")
                    filtered_result.append((unit_name, bitlocker_status))
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving BitLocker status data: {e}")
        return None


def update_bitlocker_status(sshw_db_client, data):
    sql_command = """
    UPDATE ComputerAssets
    SET BitlockerStatus = %s
    WHERE UnitName = %s
    """
    try:
        for row in data:
            unit_name, bitlocker_status = row
            sshw_db_client.execute_sql(sql_command, (bitlocker_status, unit_name))
        sshw_db_client.get_connection().commit()
        logger.info("BitLocker Status Data updated successfully in ComputerAssets table.")
    except Exception as e:
        logger.error(f"Error updating data in ComputerAssets table: {e}")


def get_model(capa_db_client):
    sql_command = """
    SELECT UNIT.NAME, CSI.VALUE
    FROM UNIT
    JOIN CSI ON UNIT.UNITID = CSI.UNITID
    WHERE CSI.SECTION = 'Randers Kommune' AND CSI.NAME = 'WSName'
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_db_client.execute_sql(sql_command)
        if result:
            filtered_result = []
            for row in result:
                unit_name, model = row
                if not (unit_name.startswith('DQ') or unit_name.startswith('AP')):
                    logger.info(f"Unit Name: {unit_name}, Model: {model}")
                    filtered_result.append((unit_name, model))
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving model data: {e}")
        return None


def update_model(sshw_db_client, data):
    sql_command = """
    UPDATE ComputerAssets
    SET Model = %s
    WHERE UnitName = %s
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        for unit_name, model in data:
            sshw_db_client.execute_sql(sql_command, (model, unit_name))
            logger.info(f"Updated Unit Name: {unit_name} with Model: {model}")
        logger.info("All updates completed successfully.")
        return "SUCCESS"
    except Exception as e:
        logger.error(f"Error updating model data: {e}")
        return None


def get_fullname(capa_db_client):
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
        result = capa_db_client.execute_sql(sql_command)
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


def update_fullname(sshw_db_client, data):
    sql_command = """
    UPDATE ComputerAssets
    SET PrimaryFullName = %s
    WHERE UnitName = %s
    """
    try:
        for row in data:
            if len(row) == 2:
                pc_unit_name, fullname = row
                sshw_db_client.execute_sql(sql_command, (fullname, pc_unit_name))
                logger.info(f"Updated Unit Name: {pc_unit_name} with Full Name: {fullname}")
            else:
                logger.error(f"Unexpected row format: {row}")
        sshw_db_client.get_connection().commit()
        logger.info("Full Name Data updated successfully in ComputerAssets table.")
    except Exception as e:
        logger.error(f"Error updating data in ComputerAssets table: {e}")
