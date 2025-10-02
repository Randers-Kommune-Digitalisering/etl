import io
import logging
from utils.sftp_connection import get_asset_sftp_client
from io import StringIO
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from utils.api_requests import APIClient
from utils.config import ATEA_API_KEY, ATEA_URL, TOPDESK_API_USERNAME, TOPDESK_API_PASSWORD, TOPDESK_API_URL, TOPDESK_ASSET_FILENAME
from utils.utils import df_to_csv_bytes

logger = logging.getLogger(__name__)

atea_client = APIClient(base_url=ATEA_URL, api_key=ATEA_API_KEY, use_subkey=True)


def create_asset_management_table_if_not_exists(db_client):
    check_table_sql = """
    CREATE TABLE IF NOT EXISTS Asset (
        UnitName VARCHAR(255) PRIMARY KEY,
        Producent VARCHAR(255),
        Model VARCHAR(255),
        Enhedstype VARCHAR(255),
        Serienummer VARCHAR(255),
        KøbsEANnr VARCHAR(255),
        AfdelingsEAN VARCHAR(255),
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
        LanMACAdresse VARCHAR(255),
        DeviceLicense VARCHAR(255),
        Drift VARCHAR(255),
        Price VARCHAR(255),
        OrderDate VARCHAR(255),
        Warranty VARCHAR(255)
    );
    """
    try:
        db_client.execute_sql(check_table_sql)
        logger.info("Checked and created Asset table if not exists.")
    except Exception as e:
        logger.error(f"Error creating Asset table: {e}")


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
            logger.error("No Unit and Serial Number found.")
            return None
    except Exception as e:
        logger.error(f"Error retrieving Asset data: {e}")
        return None


def insert_serial_numbers(db_client, data):
    check_sql_command = """
    SELECT COUNT(*)
    FROM Asset
    WHERE UnitName = :unit_name
    """
    insert_sql_command = """
    INSERT INTO Asset (UnitName, Serienummer)
    VALUES (:unit_name, :serial_number)
    """
    try:
        for row in data:
            unit_name, serial_number = row
            result = db_client.execute_sql(check_sql_command, {'unit_name': unit_name})
            if result and result[0][0] == 0:
                db_client.execute_sql(insert_sql_command, {'unit_name': unit_name, 'serial_number': serial_number})
        logger.info("Serial number data inserted successfully into Asset table.")
    except Exception as e:
        logger.error(f"Error inserting data into Asset table: {e}")


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
            logger.error("No Primary User data found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving primary user data: {e}")
        return None


def update_primary_user(db_client, data):
    sql_command = """
    UPDATE Asset
    SET PrimaryUser = :user_name
    WHERE UnitName = :unit_name
    """
    try:
        for row in data:
            unit_name, user_name = row
            db_client.execute_sql(sql_command, {'user_name': user_name, 'unit_name': unit_name})
        logger.info("Primary User Data updated successfully in Asset table.")
    except Exception as e:
        logger.error(f"Error updating data in Asset table: {e}")


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
                        department_lower = department.lower() if isinstance(department, str) else department
                        logger.info(f"PC Unit Name: {pc_unit_name}, Department: {department_lower}")
                        filtered_result.append((pc_unit_name, department_lower))
                else:
                    logger.error(f"Unexpected row format: {row}")
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No Department data found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving department data: {e}")
        return None


def update_department(db_client, data):
    sql_command = """
    UPDATE Asset
    SET Afdeling = :department
    WHERE UnitName = :unit_name
    """
    try:
        for row in data:
            if len(row) == 2:
                pc_unit_name, department = row
                department_lower = department.lower() if isinstance(department, str) else department
                db_client.execute_sql(sql_command, {'department': department_lower, 'unit_name': pc_unit_name})
            else:
                logger.error(f"Unexpected row format: {row}")
        logger.info("Department Data updated successfully in Asset table.")
    except Exception as e:
        logger.error(f"Error updating data in Asset table: {e}")


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
            logger.error("No Full Name data found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving full name data: {e}")
        return None


def update_fullname(db_client, data):
    sql_command = """
    UPDATE Asset
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
        logger.info("Full Name Data updated successfully in Asset table.")
    except Exception as e:
        logger.error(f"Error updating data in Asset table: {e}")


def get_device_type(db_client):
    sql_command = """
    SELECT UNIT.NAME, DEVICETYPE.HWNAME
    FROM UNIT
    JOIN DEVICETYPE ON UNIT.DEVICETYPEID = DEVICETYPE.ID
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = db_client.execute_sql(sql_command)
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
            logger.error("No Device Type data found.")
            return "NONE"
    except Exception as e:
        db_client.logger.error(f"Error retrieving device type data: {e}")
        return None


def update_device_type(db_client, data):
    sql_command = """
    UPDATE Asset
    SET Enhedstype = :device_type
    WHERE UnitName = :unit_name
    """
    try:
        for row in data:
            unit_name, device_type = row
            db_client.execute_sql(sql_command, {'device_type': device_type, 'unit_name': unit_name})
        db_client.get_connection().commit()
        logger.info("Device Type Data updated successfully in Asset table.")
    except Exception as e:
        logger.error(f"Error updating data in Asset table: {e}")


def get_producent(db_client):
    sql_command = """
    SELECT UNIT.NAME, INV.VALUE
    FROM UNIT
    JOIN INV ON UNIT.UNITID = INV.UNITID
    WHERE (INV.SECTION = 'System') AND (INV.NAME = 'Manufacturer')
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = db_client.execute_sql(sql_command)
        if result:
            filtered_result = []
            for row in result:
                unit_name, producent = row
                if producent and not (unit_name.startswith('DQ') or unit_name.startswith('AP')):
                    logger.info(f"Unit Name: {unit_name}, Producent: {producent}")
                    filtered_result.append(row)
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No Producent data found.")
            return "NONE"
    except Exception as e:
        db_client.logger.error(f"Error retrieving producent data: {e}")
        return None


def update_producent(db_client, data):
    sql_command = """
    UPDATE Asset
    SET Producent = :producent
    WHERE UnitName = :unit_name
    """
    try:
        for row in data:
            unit_name, producent = row
            db_client.execute_sql(sql_command, {'producent': producent, 'unit_name': unit_name})
        db_client.get_connection().commit()
        logger.info("Producent Data updated successfully in Asset table.")
    except Exception as e:
        logger.error(f"Error updating data in Asset table: {e}")


def get_os(db_client):
    sql_command = """
    SELECT UNIT.NAME, INV.VALUE
    FROM UNIT
    JOIN INV ON UNIT.UNITID = INV.UNITID
    WHERE INV.SECTION = 'Operating System' AND INV.NAME = 'System'
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = db_client.execute_sql(sql_command)
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
            logger.error("No OS Deployment data found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving OS deployment data: {e}")
        return None


def update_os(db_client, data):
    sql_command = """
    UPDATE Asset
    SET OSVersion = :os_value
    WHERE UnitName = :unit_name
    """
    try:
        for row in data:
            unit_name, os_value = row
            db_client.execute_sql(sql_command, {'os_value': os_value, 'unit_name': unit_name})
        db_client.get_connection().commit()
        logger.info("OS Deployment Data updated successfully in Asset table.")
    except Exception as e:
        logger.error(f"Error updating data in Asset table: {e}")


def get_last_online(db_client):
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
        result = db_client.execute_sql(sql_command)
        if result:
            filtered_result = []
            for row in result:
                unit_name, last_online = row
                logger.info(f"Unit Name: {unit_name}, Last Online: {last_online}")
                filtered_result.append((unit_name, last_online))
            logger.info(f"Total elements: {len(filtered_result)}")
            return filtered_result
        else:
            logger.error("No Last Online data found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving last online data: {e}")
        return None


def update_last_online(db_client, data):
    sql_command = """
    UPDATE Asset
    SET SidsteLoginDato = :last_online
    WHERE UnitName = :unit_name
    """
    try:
        for row in data:
            unit_name, last_online = row
            db_client.execute_sql(sql_command, {'last_online': last_online, 'unit_name': unit_name})
        db_client.get_connection().commit()
        logger.info("Last Online Data updated successfully in Asset table.")
    except Exception as e:
        logger.error(f"Error updating data in Asset table: {e}")


def get_last_install_date(db_client):
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
        result = db_client.execute_sql(sql_command)
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
            logger.error("No Get Last Install Date data found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving last install date data: {e}")
        return None


def update_last_install_date(db_client, data):
    sql_command = """
    UPDATE Asset
    SET SidsteRul = :last_install_date
    WHERE UnitName = :unit_name
    """
    try:
        for row in data:
            unit_name, last_install_date = row
            db_client.execute_sql(sql_command, {'last_install_date': last_install_date, 'unit_name': unit_name})
        db_client.get_connection().commit()
        logger.info("Last Install Date Data updated successfully in Asset table.")
    except Exception as e:
        logger.error(f"Error updating data in Asset table: {e}")


def update_drift_status(db_client):
    try:
        sql_select = "SELECT UnitName, SidsteLoginDato FROM Asset"
        result = db_client.execute_sql(sql_select)

        if not result:
            logger.info("No units found in Asset table.")
            return

        six_months_ago = datetime.now() - relativedelta(months=6)

        for unit_name, last_login_str in result:
            drift_status = "FALSE"

            if last_login_str:
                try:
                    last_login = parse(str(last_login_str))
                    if last_login >= six_months_ago:
                        drift_status = "TRUE"
                except Exception:
                    logger.error(f"Could not parse SidsteLoginDato: {last_login_str} for {unit_name}")

            sql_update = """
            UPDATE Asset
            SET Drift = :drift_status
            WHERE UnitName = :unit_name
            """
            db_client.execute_sql(sql_update, {
                'drift_status': drift_status,
                'unit_name': unit_name
            })
            logger.info(f"Updated Drift status for Unit Name: {unit_name} to {drift_status}")

        db_client.get_connection().commit()
        logger.info("Drift status updated for all units.")

    except Exception as e:
        logger.error(f"Error updating Drift status for all units: {e}")


def get_lan_mac_addresses(db_client):
    lan_mac_sql = """
    SELECT U.NAME, MAC_INV.VALUE
    FROM UNIT U
    JOIN INV MAC_INV ON U.UNITID = MAC_INV.UNITID
        AND MAC_INV.SECTION = 'Network Adapter'
        AND (
            MAC_INV.NAME = 'Device #1 MAC Address' OR
            MAC_INV.NAME = 'Device #2 MAC Address' OR
            MAC_INV.NAME = 'Device #3 MAC Address' OR
            MAC_INV.NAME = 'Device #4 MAC Address' OR
            MAC_INV.NAME = 'Device #5 MAC Address' OR
            MAC_INV.NAME = 'Device #6 MAC Address' OR
            MAC_INV.NAME = 'Device #7 MAC Address' OR
            MAC_INV.NAME = 'Device #8 MAC Address' OR
            MAC_INV.NAME = 'Device #9 MAC Address' OR
            MAC_INV.NAME = 'Device #10 MAC Address' OR
            MAC_INV.NAME = 'Device #11 MAC Address' OR
            MAC_INV.NAME = 'Device #12 MAC Address' OR
            MAC_INV.NAME = 'Device #13 MAC Address' OR
            MAC_INV.NAME = 'Device #14 MAC Address' OR
            MAC_INV.NAME = 'Device #15 MAC Address'
        )
    JOIN INV IP_INV ON U.UNITID = IP_INV.UNITID
        AND IP_INV.SECTION = 'Network Configuration'
        AND (
            (MAC_INV.NAME = 'Device #1 MAC Address' AND IP_INV.NAME = 'Device #1 IP address')
            OR (MAC_INV.NAME = 'Device #2 MAC Address' AND IP_INV.NAME = 'Device #2 IP address')
            OR (MAC_INV.NAME = 'Device #3 MAC Address' AND IP_INV.NAME = 'Device #3 IP address')
            OR (MAC_INV.NAME = 'Device #4 MAC Address' AND IP_INV.NAME = 'Device #4 IP address')
            OR (MAC_INV.NAME = 'Device #5 MAC Address' AND IP_INV.NAME = 'Device #5 IP address')
            OR (MAC_INV.NAME = 'Device #6 MAC Address' AND IP_INV.NAME = 'Device #6 IP address')
            OR (MAC_INV.NAME = 'Device #7 MAC Address' AND IP_INV.NAME = 'Device #7 IP address')
            OR (MAC_INV.NAME = 'Device #8 MAC Address' AND IP_INV.NAME = 'Device #8 IP address')
            OR (MAC_INV.NAME = 'Device #9 MAC Address' AND IP_INV.NAME = 'Device #9 IP address')
            OR (MAC_INV.NAME = 'Device #10 MAC Address' AND IP_INV.NAME = 'Device #10 IP address')
            OR (MAC_INV.NAME = 'Device #11 MAC Address' AND IP_INV.NAME = 'Device #11 IP address')
            OR (MAC_INV.NAME = 'Device #12 MAC Address' AND IP_INV.NAME = 'Device #12 IP address')
            OR (MAC_INV.NAME = 'Device #13 MAC Address' AND IP_INV.NAME = 'Device #13 IP address')
            OR (MAC_INV.NAME = 'Device #14 MAC Address' AND IP_INV.NAME = 'Device #14 IP address')
            OR (MAC_INV.NAME = 'Device #15 MAC Address' AND IP_INV.NAME = 'Device #15 IP address')
        )
    WHERE IP_INV.VALUE LIKE '10.129.%'
       OR IP_INV.VALUE LIKE '10.146.%'
       OR IP_INV.VALUE LIKE '10.161.%'
       OR IP_INV.VALUE LIKE '10.177.%'
    """
    logger.info(f"Executing LAN MAC SQL command: {lan_mac_sql}")
    try:
        result = db_client.execute_sql(lan_mac_sql)
        filtered_result = []
        for row in result:
            unit_name, lan_mac_address = row
            if not (unit_name.startswith('DQ') or unit_name.startswith('AP')):
                logger.info(f"Unit Name: {unit_name}, LAN MAC Address: {lan_mac_address}")
                filtered_result.append((unit_name, lan_mac_address))
        logger.info(f"Total LAN MAC elements: {len(filtered_result)}")
        return filtered_result
    except Exception as e:
        logger.error(f"Error retrieving LAN MAC addresses data: {e}")
        return None


def update_lan_mac_addresses(db_client, data):
    sql_command = """
    UPDATE Asset
    SET LANMacAdresse = :lan_mac_address
    WHERE UnitName = :unit_name
    """
    try:
        for row in data:
            unit_name, lan_mac_address = row
            db_client.execute_sql(sql_command, {'lan_mac_address': lan_mac_address, 'unit_name': unit_name})
        db_client.get_connection().commit()
        logger.info("LAN MAC Addresses Data updated successfully in Asset table.")
    except Exception as e:
        logger.error(f"Error updating LAN MAC data in Asset table: {e}")


def get_mac_addresses(db_client):
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
        result = db_client.execute_sql(sql_command)
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
            logger.error("No MAC Addresses data found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving MAC addresses data: {e}")
        return None


def update_mac_addresses(db_client, data):
    sql_command = """
    UPDATE Asset
    SET MACAdresse = :mac_addresses
    WHERE UnitName = :unit_name
    """
    try:
        for row in data:
            unit_name, mac_addresses = row
            db_client.execute_sql(sql_command, {'mac_addresses': mac_addresses, 'unit_name': unit_name})
        db_client.get_connection().commit()
        logger.info("MAC Addresses Data updated successfully in Asset table.")
    except Exception as e:
        logger.error(f"Error updating data in Asset table: {e}")


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
            logger.error("No Bitlocker Code data found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving BitLocker Code data: {e}")
        return None


def update_bitlocker_code(db_client, data):
    sql_command = """
    UPDATE Asset
    SET BitlockerKode = :bitlocker_code
    WHERE UnitName = :unit_name
    """
    try:
        for row in data:
            if len(row) == 2:
                unit_name, bitlocker_code = row
                db_client.execute_sql(sql_command, {'bitlocker_code': bitlocker_code, 'unit_name': unit_name})
            else:
                logger.error(f"Unexpected row format: {row}")
        db_client.get_connection().commit()
        logger.info("BitLocker Code Data updated successfully in Asset table.")
    except Exception as e:
        logger.error(f"Error updating data in Asset table: {e}")


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
            logger.error("No BitLocker Encryption data found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving BitLocker Encryption data: {e}")
        return None


def update_bitlocker_encryption(db_client, data):
    sql_command = """
    UPDATE Asset
    SET BitlockerKrypteringProcent = :bitlocker_status
    WHERE UnitName = :unit_name
    """
    try:
        for row in data:
            unit_name, bitlocker_status = row
            db_client.execute_sql(sql_command, {'bitlocker_status': bitlocker_status, 'unit_name': unit_name})
        db_client.get_connection().commit()
        logger.info("BitLocker Encryption Data updated successfully in Asset table.")
    except Exception as e:
        logger.error(f"Error updating data in Asset table: {e}")


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
            logger.error("No Bitlocker Status data found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving BitLocker status data: {e}")
        return None


def update_bitlocker_status(db_client, data):
    sql_command = """
    UPDATE Asset
    SET BitlockerStatus = :bitlocker_status
    WHERE UnitName = :unit_name
    """
    try:
        for row in data:
            unit_name, bitlocker_status = row
            db_client.execute_sql(sql_command, {'bitlocker_status': bitlocker_status, 'unit_name': unit_name})
        db_client.get_connection().commit()
        logger.info("BitLocker Status Data updated successfully in Asset table.")
    except Exception as e:
        logger.error(f"Error updating BitLocker Status in Asset table: {e}")


def get_model(db_client):
    sql_command = """
    SELECT UNIT.NAME, CSI.VALUE
    FROM UNIT
    JOIN CSI ON UNIT.UNITID = CSI.UNITID
    WHERE CSI.SECTION = 'Randers Kommune' AND CSI.NAME = 'WSName'
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = db_client.execute_sql(sql_command)
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
            logger.error("No Model data found.")
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving model data: {e}")
        return None


def update_model(db_client, data):
    sql_command = """
    UPDATE Asset
    SET Model = :model
    WHERE UnitName = :unit_name
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        for unit_name, model in data:
            db_client.execute_sql(sql_command, {'model': model, 'unit_name': unit_name})
            logger.info(f"Updated Unit Name: {unit_name} with Model: {model}")
        logger.info("Model Data updated successfully in Asset table.")
    except Exception as e:
        logger.error(f"Error updating Model Data in Asset table: {e}")
        return None


def update_device_license(db_client, unit_name):
    try:
        sql_command = """
        UPDATE Asset
        SET DeviceLicense = :device_license
        WHERE UnitName = :unit_name
        """
        db_client.execute_sql(sql_command, {'device_license': 'TRUE', 'unit_name': unit_name})
        logger.info(f"Updated Device License for Unit Name: {unit_name}")
    except Exception as e:
        logger.error(f"Error updating device license for {unit_name}: {e}")


def get_all_unit_names(db_client):
    try:
        sql_command = "SELECT UnitName FROM Asset"
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
            logger.info("No unit names found in Asset table.")
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


def fetch_atea_data():
    try:
        logger.info("Fetching data from Atea API...")
        url = "/api/assets/search?PageSize=2000&Page=1&AssetType=AB,AA"
        response = atea_client.make_request(path=url, method='get')

        if isinstance(response, list):
            logger.info(f"Successfully retrieved data from Atea API. Total records: {len(response)}")
            return response
        else:
            logger.error(f"Unexpected response structure: {response}")
            return None
    except Exception as e:
        logger.error(f"Error while fetching data from Atea API: {e}")
        return None


def create_serial_price_map(atea_data):
    if not atea_data:
        logger.info("No data retrieved from Atea API.")
        return {}

    serial_price_map = {
        item.get('SerialNumber'): item.get('Price')
        for item in atea_data
        if item.get('SerialNumber') and item.get('Price')
    }
    logger.info(f"Retrieved {len(serial_price_map)} serial numbers with prices from Atea API.")
    logger.debug(f"Atea Serial-Price Map: {serial_price_map}")
    return serial_price_map


def create_serial_info_map(atea_data):
    if not atea_data:
        logger.info("No data retrieved from Atea API.")
        return {}

    serial_info_map = {
        item.get('SerialNumber'): {
            'Price': item.get('Price'),
            'OrderDate': item.get('OrderDate'),
            'Warranty': item.get('Warranty')
        }
        for item in atea_data
        if item.get('SerialNumber') and item.get('Price') and item.get('OrderDate') and item.get('Warranty')
    }
    logger.info(f"Retrieved {len(serial_info_map)} serial numbers with prices, order and warranty dates from Atea API.")
    logger.info(f"Atea Serial-Info Map: {serial_info_map}")
    return serial_info_map


def fetch_database_serial_numbers(db_client):
    try:
        sql_command = "SELECT Serienummer FROM Asset"
        db_serial_numbers = db_client.execute_sql(sql_command)
        if not db_serial_numbers:
            logger.info("No serial numbers found in the database.")
            return []

        db_serial_number_list = [serial_number[0] for serial_number in db_serial_numbers]
        logger.info(f"Retrieved {len(db_serial_number_list)} serial numbers from the database.")
        logger.debug(f"Database Serial Numbers: {db_serial_number_list}")
        return db_serial_number_list
    except Exception as e:
        logger.error(f"Error fetching serial numbers from the database: {e}")
        return []


def update_prices_orderdate_warranty_in_database(db_client, matching_serials, serial_info_map):
    try:
        update_sql_command = """
        UPDATE Asset
        SET Price = :price, OrderDate = :order_date, Warranty = :warranty
        WHERE TRIM(LOWER(Serienummer)) = TRIM(LOWER(:serial_number))
        """

        for serial_number in matching_serials:
            info = serial_info_map[serial_number]
            price_str = "{:.2f}".format(float(info['Price']))
            order_date = format_atea_date(info['OrderDate'])
            warranty = format_atea_date(info['Warranty'])
            norm_serial = serial_number.strip().lower() if serial_number else serial_number
            logger.info(f"Updating Serial Number: '{norm_serial}', Price: {price_str}, OrderDate: {order_date}, Warranty: {warranty}")
            db_client.execute_sql(
                update_sql_command,
                {
                    'price': price_str,
                    'order_date': order_date,
                    'warranty': warranty,
                    'serial_number': norm_serial
                }
            )

        db_client.get_connection().commit()
        logger.info("Price, OrderDate and Warranty updates completed successfully.")
    except Exception as e:
        logger.error(f"Error updating prices, order dates and warranty in the database: {e}")


def update_asset_info_from_atea(db_client):
    try:
        atea_data = fetch_atea_data()
        if not atea_data:
            return

        serial_price_map = create_serial_info_map(atea_data)

        db_serial_numbers = fetch_database_serial_numbers(db_client)
        if not db_serial_numbers:
            return

        matching_serials = set(db_serial_numbers) & set(serial_price_map.keys())

        logger.info(f"Matching Serial Numbers: {matching_serials}")

        if matching_serials:
            update_prices_orderdate_warranty_in_database(db_client, matching_serials, serial_price_map)
        else:
            logger.info("No matching serial numbers found. No updates performed.")
    except Exception as e:
        logger.error(f"Error in Updating price from Atea: {e}")


def format_atea_date(date_str):
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%dT%H:%M:%S")
        return dt.strftime("%Y-%m-%dT%H:%M:%S.00")
    except Exception as e:
        logger.error(f"Error formatting date '{date_str}': {e}")
        return date_str


def format_order_date(date_str):
    if not date_str:
        return None
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S")
        return dt.strftime("%Y-%m-%dT%H:%M:%S.00")
    except Exception as e:
        logger.error(f"Error formatting order date '{date_str}': {e}")
        return date_str


def update_historical_data_from_comm2ig(db_client, sftp_file_path):
    try:
        csv_data = download_csv_from_sftp(sftp_file_path)
        df = pd.read_csv(io.StringIO(csv_data), dtype=str, sep=',')
        df.columns = df.columns.str.strip()
        required_cols = ['Serienr.', 'Pris pr.stk. i kr. ekskl. moms', 'Fakturadato', 'EAN-nr.']
        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            logger.error(f"Required columns not found in CSV file: {missing}")
            logger.info(f"Available columns: {df.columns.tolist()}")
            return False

        db_serials = fetch_database_serial_numbers(db_client)
        db_serials_norm = set(s.lstrip('sS') if isinstance(s, str) else s for s in db_serials)

        updated = 0
        not_updated_serials = []
        for _, row in df.iterrows():
            serial = row['Serienr.']
            if isinstance(serial, str) and serial.startswith('S'):
                serial_norm = serial[1:]
            else:
                serial_norm = serial
            price = row['Pris pr.stk. i kr. ekskl. moms']
            fakturadato = row['Fakturadato']
            ean_nr = row['EAN-nr.'] if 'EAN-nr.' in row else None
            fakturadato_formatted = format_order_date(fakturadato)

            if serial_norm in db_serials_norm:
                try:
                    price_float = "{:.2f}".format(float(str(price).replace(',', '.')))
                except Exception:
                    logger.warning(f"Could not convert price '{price}' for serial '{serial_norm}'")
                    continue
                sql = """
                UPDATE Asset
                SET Price = :price, OrderDate = :order_date, KøbsEANnr = :ean_nr
                WHERE TRIM(LOWER(REPLACE(Serienummer, 'S', ''))) = TRIM(LOWER(:serial_norm))
                """
                db_client.execute_sql(sql, {
                    'price': price_float,
                    'order_date': fakturadato_formatted,
                    'ean_nr': ean_nr,
                    'serial_norm': serial_norm
                })
                updated += 1
                logger.info(f"Updated Serial: {serial_norm} with Price: {price_float}, OrderDate: {fakturadato_formatted}, KøbsEANnr: {ean_nr}")
            else:
                not_updated_serials.append(serial_norm)
                logger.info(f"No matching Serienummer found in DB for: {serial_norm}")

        db_client.get_connection().commit()
        logger.info(f"Updated price, order date, and KøbsEANnr for {updated} serial numbers from Comm2ig Historical data.")
        if not_updated_serials:
            logger.info(f"Serial numbers NOT updated (no match in DB): {len(not_updated_serials)} - {not_updated_serials}")
        return True
    except Exception as e:
        logger.error(f"Error updating price, order date, and KøbsEANnr from CSV: {e}")
        return False


def update_ean_from_atea(db_client, sftp_file_path):
    try:
        excel_data = download_excel_from_sftp(sftp_file_path)
        df = pd.read_excel(io.BytesIO(excel_data), dtype=str)
        logger.info(f"Excel columns: {df.columns.tolist()}")
        df.columns = df.columns.str.strip()
        if 'Nummer' not in df.columns or 'EAN-nr.' not in df.columns:
            logger.error("Excel missing 'Nummer' or 'EAN-nr.' column.")
            return False

        atea_data = fetch_atea_data()
        if not atea_data:
            logger.error("No data fetched from Atea API.")
            return False

        billto_map = {str(item.get('BillTo')): item.get('SerialNumber') for item in atea_data if item.get('BillTo') and item.get('SerialNumber')}

        updated = 0
        for _, row in df.iterrows():
            nummer = str(row['Nummer']).strip()
            ean_nr = row['EAN-nr.']
            serial = billto_map.get(nummer)
            if serial:
                sql = """
                UPDATE Asset
                SET KøbsEANnr = :ean_nr
                WHERE Serienummer = :serial
                """
                db_client.execute_sql(sql, {'ean_nr': ean_nr, 'serial': serial})
                updated += 1
                logger.info(f"KøbsEANnr. {ean_nr} updated for Serienummer: {serial} (Nummer: {nummer})")
        db_client.get_connection().commit()
        logger.info(f"KøbsEANnr. updated for {updated} rows.")
        return True
    except Exception as e:
        logger.error(f"Error updating KøbsEANnr. from Nummer/BillTo: {e}")
        return False


def update_afdelings_ean_from_delta(db_client, sftp_file_path):
    try:
        excel_data = download_excel_from_sftp(sftp_file_path)
        df = pd.read_excel(io.BytesIO(excel_data), dtype=str)
        df.columns = df.columns.str.strip()

        if 'Institution/afdeling' not in df.columns or 'Ean-nummer' not in df.columns:
            logger.error("Excel missing 'Institution/afdeling' or 'Ean-nummer' column.")
            return False

        updated = 0
        for _, row in df.iterrows():
            afdeling = str(row['Institution/afdeling']).strip().lower()
            ean_nummer = str(row['Ean-nummer']).strip()
            if not ean_nummer or ean_nummer.lower() == 'nan':
                logger.info(f"Skipping update for Afdeling: {afdeling} as Ean-nummer is empty.")
                continue
            sql = """
            UPDATE Asset
            SET AfdelingsEAN = :ean_nummer
            WHERE Afdeling = :afdeling
            """
            db_client.execute_sql(sql, {'ean_nummer': ean_nummer, 'afdeling': afdeling})
            updated += 1
            logger.info(f"AfdelingsEAN {ean_nummer} updated for Afdeling: {afdeling}")
        db_client.get_connection().commit()
        logger.info(f"AfdelingsEAN updated for {updated} rows.")
        return True
    except Exception as e:
        logger.error(f"Error updating AfdelingsEAN from excel: {e}")
        return False


def download_csv_from_sftp(sftp_file_path):
    sftp_client = get_asset_sftp_client()
    with sftp_client.get_connection() as conn:
        with conn.open(sftp_file_path, 'r') as file:
            csv_data = file.read().decode('utf-8')
    return csv_data


def download_excel_from_sftp(sftp_file_path):
    sftp_client = get_asset_sftp_client()
    with sftp_client.get_connection() as conn:
        with conn.open(sftp_file_path, 'rb') as file:
            excel_data = file.read()
    return excel_data


def upload_assets_to_topdesk(db_client):
    try:
        sql_command = "SELECT * FROM Asset"
        result = db_client.execute_sql(sql_command)
        if not result:
            logger.info("No data found in Asset table.")
            return False

        columns = [
            "UnitName", "Producent", "Model", "Enhedstype", "Serienummer", "KøbsEANnr", "AfdelingsEAN",
            "PrimaryFullName", "PrimaryUser", "Afdeling", "SidsteLoginDato",
            "SidsteRul", "BitlockerKode", "BitlockerStatus", "BitlockerKrypteringProcent",
            "OSVersion", "MACAdresse", "LanMACAdresse", "DeviceLicense", "Drift", "Price",
            "OrderDate", "Warranty"
        ]
        df = pd.DataFrame(result, columns=columns)
        csv_bytes = df_to_csv_bytes(df, sep=';', encoding='UTF-8')

        topdesk_client = APIClient(
            base_url=TOPDESK_API_URL,
            username=TOPDESK_API_USERNAME,
            password=TOPDESK_API_PASSWORD
        )

        upload_path = f"/services/import-to-api-v1/api/sourceFiles?filename={TOPDESK_ASSET_FILENAME}"

        logger.info(f"Uploading {TOPDESK_ASSET_FILENAME} to TopDesk at {TOPDESK_API_URL}{upload_path}")
        topdesk_client.make_request(path=upload_path, method="put", data=csv_bytes)
        logger.info(f"Successfully uploaded {TOPDESK_ASSET_FILENAME} to TopDesk.")
        return True
    except Exception as e:
        logger.error(f"Error uploading {TOPDESK_ASSET_FILENAME} to TopDesk: {e}")
        return False
