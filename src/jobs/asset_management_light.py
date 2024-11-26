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

        # Insert/Update Producent
        producent_result = get_producent(capa_db_client)
        if producent_result:
            for row in producent_result:
                logger.info(f"Producent: {row}")
            update_producent(sshw_db_client, producent_result)
        else:
            logger.info("No producent data found.")

        # Insert/Update Device Type
        device_type_result = get_device_type(capa_db_client)
        if device_type_result:
            for row in device_type_result:
                logger.info(f"Device Type: {row}")
            update_device_type(sshw_db_client, device_type_result)
        else:
            logger.info("No device type data found.")

        # Insert/Update OS
        os_deployment_result = get_os(capa_db_client)
        if os_deployment_result:
            for row in os_deployment_result:
                logger.info(f"OS Deployment: {row}")
            update_os_deployment(sshw_db_client, os_deployment_result)
        else:
            logger.info("No OS deployment data found.")

        # Insert/Update Last Online
        last_online_result = get_last_online(capa_db_client)
        if last_online_result:
            for row in last_online_result:
                logger.info(f"Last Online: {row}")
            update_last_online(sshw_db_client, last_online_result)
        else:
            logger.info("No last online data found.")

        # Insert/Update Default User
        default_user_result = get_default_user(capa_db_client)
        if default_user_result:
            for row in default_user_result:
                logger.info(f"Default User: {row}")
            update_default_user(sshw_db_client, default_user_result)
        else:
            logger.info("No default user data found.")

        # Insert/Update Last Install Date
        last_install_dateresult = get_last_install_date(capa_db_client)
        if last_install_dateresult:
            for row in last_install_dateresult:
                logger.info(f"Last Install Date: {row}")
            update_last_install_date(sshw_db_client, last_install_dateresult)
        else:
            logger.info("No last install date data found.")

        # Insert/Update MAC Addresses
        mac_addresses_result = get_mac_addresses(capa_db_client)
        if mac_addresses_result:
            for row in mac_addresses_result:
                logger.info(f"MAC Addresses: {row}")
            update_mac_addresses(sshw_db_client, mac_addresses_result)
        else:
            logger.info("No MAC addresses data found.")

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


def update_os_deployment(sshw_db_client, data):
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
           FORMAT(DATEADD(HOUR, 1, DATEADD(SECOND, TRY_CAST(UNIT.LASTONLINE AS BIGINT), '1970-01-01')), 'yyyy-MM-dd HH:mm:ss') AS LASTONLINE
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


def get_default_user(capa_db_client):
    sql_command = """
    SELECT UNIT.NAME, LGI.VALUE
    FROM UNIT
    JOIN LGI ON UNIT.UNITID = LGI.UNITID
    WHERE LGI.SECTION = 'Default User' AND LGI.NAME = 'User Name'
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_db_client.execute_sql(sql_command)
        if result:
            filtered_result = []
            for row in result:
                unit_name, user_name = row
                if not (unit_name.startswith('DQ') or unit_name.startswith('AP')):
                    logger.info(f"Unit Name: {unit_name}, Default User: {user_name}")
                    filtered_result.append((unit_name, user_name))
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving default user data: {e}")
        return None


def update_default_user(sshw_db_client, data):
    sql_command = """
    UPDATE ComputerAssets
    SET DefaultUser = %s
    WHERE UnitName = %s
    """
    try:
        for row in data:
            unit_name, user_name = row
            sshw_db_client.execute_sql(sql_command, (user_name, unit_name))
        sshw_db_client.get_connection().commit()
        logger.info("Default User Data updated successfully in ComputerAssets table.")
    except Exception as e:
        logger.error(f"Error updating data in ComputerAssets table: {e}")


def get_last_install_date(capa_db_client):
    sql_command = """
    SELECT UNIT.NAME,
           FORMAT(DATEADD(SECOND, TRY_CAST(INV.VALUE AS BIGINT), '1970-01-01'), 'yyyy-MM-dd HH:mm:ss') AS LAST_INSTALL_DATE
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
