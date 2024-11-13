import logging
from utils.database_client import DatabaseClient
from utils.config import (
    ASSET_MANAGEMENT_LIGHT_DB_HOST,
    ASSET_MANAGEMENT_LIGHT_DB_USER,
    ASSET_MANAGEMENT_LIGHT_DB_PASS,
    ASSET_MANAGEMENT_LIGHT_DB_DATABASE
)

db_client = DatabaseClient(
    database=ASSET_MANAGEMENT_LIGHT_DB_DATABASE,
    username=ASSET_MANAGEMENT_LIGHT_DB_USER,
    password=ASSET_MANAGEMENT_LIGHT_DB_PASS,
    host=ASSET_MANAGEMENT_LIGHT_DB_HOST
)
logger = logging.getLogger(__name__)


def job():
    try:
        logger.info("Starting Asset-Management-Light job")
        return True
    except Exception as e:
        logger.error(f"Error in Asset-Management-Light job: {e}")
        return False


def get_capa_data(db_client, in_section, in_dataname, in_computername):
    sql_command = f"""
    SELECT INV.VALUE
    FROM UNIT
    JOIN INV ON UNIT.UNITID = INV.UNITID
    WHERE INV.SECTION = '{in_section}'
    AND UNIT.NAME = '{in_computername}'
    AND INV.NAME = '{in_dataname}'
    """

    try:
        result = db_client.execute_sql(sql_command)
        if result:
            return result[0][0]
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        db_client.logger.error(f"Error retrieving capa data: {e}")
        return None


section = 'CapaInstaller'
dataname = 'Last Agent Execution'
computername = 'DQ12223'
capa_data = get_capa_data(db_client, section, dataname, computername)
logger.info(f"Capa Data: {capa_data}")


def get_capa_csi_data(db_client, in_section, in_dataname, in_computername):
    sql_command = f"""
    SELECT CSI.VALUE
    FROM UNIT
    JOIN CSI ON UNIT.UNITID = CSI.UNITID
    WHERE CSI.SECTION = '{in_section}'
    AND UNIT.NAME = '{in_computername}'
    AND CSI.NAME = '{in_dataname}'
    """

    try:
        result = db_client.execute_sql(sql_command)
        if result:
            return result[0][0]
        else:
            return "NONE"
    except Exception as e:
        db_client.logger.error(f"Error retrieving capa CSI data: {e}")
        return None


def get_capa_lgi_data(db_client, in_section, in_dataname, in_computername):
    sql_command = f"""
    SELECT LGI.VALUE
    FROM UNIT
    JOIN LGI ON UNIT.UNITID = LGI.UNITID
    WHERE LGI.SECTION = '{in_section}'
    AND UNIT.NAME = '{in_computername}'
    AND LGI.NAME = '{in_dataname}'
    """
    try:
        result = db_client.execute_sql(sql_command)
        if result:
            return result[0][0]
        else:
            return "NONE"
    except Exception as e:
        logger.error(f"Error retrieving capa LGI data: {e}")
        return None


def check_net_manu(inmanu):
    inmanu = inmanu.upper()
    if "WAN MINIPORT" in inmanu or "BLUETOOTH" in inmanu or "MICROSOFT WI-FI DIRECT VIRTUAL ADAPTER" in inmanu:
        return False
    return True


def sshw_pcid_name(db_client, inname):
    sql_command = f"SELECT id FROM pcinfo WHERE name = '{inname}'"
    try:
        result = db_client.execute_sql(sql_command)
        if result and len(result) > 0:
            return result[0][0]
        else:
            return "0"
    except Exception as e:
        logger.error(f"Error in sshw_pcid_name: {e}")
        return "0"
