import logging
from utils.database_client import DatabaseClient
from utils.db_creation import create_hwdb
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
        if create_hwdb():
            logger.info("Database created successfully")
        else:
            logger.error("Failed to create database")
        return True
    except Exception as e:
        logger.error(f"Error in Asset-Management-Light job: {e}")
        return False


def get_capa_data(capa_db_client, in_section, in_dataname, in_computername):
    sql_command = f"""
    SELECT INV.VALUE
    FROM UNIT
    JOIN INV ON UNIT.UNITID = INV.UNITID
    WHERE INV.SECTION = '{in_section}'
    AND UNIT.NAME = '{in_computername}'
    AND INV.NAME = '{in_dataname}'
    """

    try:
        result = capa_db_client.execute_sql(sql_command)
        if result:
            return result[0][0]
        else:
            logger.error("No results found.")
            return "NONE"
    except Exception as e:
        capa_db_client.logger.error(f"Error retrieving capa data: {e}")
        return None


section = 'CapaInstaller'
dataname = 'Last Agent Execution'
computername = 'DQ12223'
capa_data = get_capa_data(capa_db_client, section, dataname, computername)
logger.info(f"Capa Data: {capa_data}")


def get_capa_csi_data(capa_db_client, in_section, in_dataname, in_computername):
    sql_command = f"""
    SELECT CSI.VALUE
    FROM UNIT
    JOIN CSI ON UNIT.UNITID = CSI.UNITID
    WHERE CSI.SECTION = '{in_section}'
    AND UNIT.NAME = '{in_computername}'
    AND CSI.NAME = '{in_dataname}'
    """

    try:
        result = capa_db_client.execute_sql(sql_command)
        if result:
            return result[0][0]
        else:
            return "NONE"
    except Exception as e:
        capa_db_client.logger.error(f"Error retrieving capa CSI data: {e}")
        return None


def get_capa_lgi_data(capa_db_client, in_section, in_dataname, in_computername):
    sql_command = f"""
    SELECT LGI.VALUE
    FROM UNIT
    JOIN LGI ON UNIT.UNITID = LGI.UNITID
    WHERE LGI.SECTION = '{in_section}'
    AND UNIT.NAME = '{in_computername}'
    AND LGI.NAME = '{in_dataname}'
    """
    try:
        result = capa_db_client.execute_sql(sql_command)
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


def sshw_pcid_mac(sshw_db_client, inmac):
    try:
        sql_command = f"SELECT pcid FROM mac WHERE macadr = '{inmac}'"
        result = sshw_db_client.execute_sql(sql_command)
        if result and len(result) > 0:
            return result[0][0]
        else:
            return "0"
    except Exception as e:
        logger.error(f"Error in sshw_pcid_mac: {e} SQL: {sql_command}")
        return "0"


def sshw_new_pcinfo(sshw_db_client, inname, instate, inagedate, inshortsite, inbiosver, inbiosdato, inadfqdn, inadgroups, inmodel, inmanufactor, inserialnr, inOS, inRam, inProcessortype, inProcessorspeed, inDiskModel, inDiskSize, inTPMactivated, inTPMowned, inTPMenabled, inTPMversion, inDefaultUser, inBitlockerReady, inSSHwtype, inadlastlogon, ininstalldate):
    try:
        inSSHwtypeval = ", SSHwtype" if inSSHwtype else ""
        inSSHwtypedat = f", '{inSSHwtype}'" if inSSHwtype else ""

        inagedateval = ", agedate" if inagedate else ""
        inagedatedat = f", '{inagedate}'" if inagedate else ""

        inadlastlogonval = ", adlastlogon" if inadlastlogon else ""
        inadlastlogondat = f", '{inadlastlogon}'" if inadlastlogon else ""

        ininstalldateval = ", lastinstall" if ininstalldate else ""
        ininstalldatedat = f", '{ininstalldate}'" if ininstalldate else ""

        sql_command = f"""
        INSERT INTO pcinfo (name, state{inagedateval}, shortsite, biosver, biosdato, adfqdn, adgroups, model, manufactor, serialnr, OS, Ram, Processortype, Processorspeed, DiskModel, DiskSize, TPMactivated, TPMowned, TPMenabled, TPMversion, DefaultUser, BitlockerReady{inSSHwtypeval}{inadlastlogonval}{ininstalldateval})
        VALUES ('{inname}', '{instate}'{inagedatedat}, '{inshortsite}', '{inbiosver}', '{inbiosdato}', '{inadfqdn}', '{inadgroups}', '{inmodel}', '{inmanufactor}', '{inserialnr}', '{inOS}', '{inRam}', '{inProcessortype}', '{inProcessorspeed}', '{inDiskModel}', '{inDiskSize}', '{inTPMactivated}', '{inTPMowned}', '{inTPMenabled}', '{inTPMversion}', '{inDefaultUser}', '{inBitlockerReady}'{inSSHwtypedat}{inadlastlogondat}{ininstalldatedat})
        """

        sshw_db_client.execute_sql(sql_command)
    except Exception as e:
        logger.error(f"Error in sshw_new_pcinfo: {e} SQL: {sql_command}")


def sshw_upd_pcinfo(sshw_db_client, inid, inname, instate, inagedate, inshortsite, inbiosver, inbiosdato, inadfqdn, inadgroups, inmodel, inmanufactor, inserialnr, inOS, inRam, inProcessortype, inProcessorspeed, inDiskModel, inDiskSize, inTPMactivated, inTPMowned, inTPMenabled, inTPMversion, inDefaultUser, inBitlockerReady, inSSHwtype, inadlastlogon, ininstalldate):
    try:
        inSSHwtypevaldat = f", SSHwtype = '{inSSHwtype}'" if inSSHwtype else ""
        inagedatevaldat = f", agedate = '{inagedate}'" if inagedate else ""
        inadlastlogonvaldat = f", adlastlogon = '{inadlastlogon}'" if inadlastlogon else ""
        ininstalldatevaldat = f", lastinstall = '{ininstalldate}'" if ininstalldate else ""

        sql_command = f"""
        UPDATE pcinfo SET
        name = '{inname}',
        state = '{instate}'
        {inagedatevaldat},
        shortsite = '{inshortsite}',
        biosver = '{inbiosver}',
        biosdato = '{inbiosdato}',
        adfqdn = '{inadfqdn}',
        adgroups = '{inadgroups}',
        model = '{inmodel}',
        manufactor = '{inmanufactor}',
        serialnr = '{inserialnr}',
        OS = '{inOS}',
        Ram = '{inRam}',
        Processortype = '{inProcessortype}',
        Processorspeed = '{inProcessorspeed}',
        DiskModel = '{inDiskModel}',
        DiskSize = '{inDiskSize}',
        TPMactivated = '{inTPMactivated}',
        TPMowned = '{inTPMowned}',
        TPMenabled = '{inTPMenabled}',
        TPMversion = '{inTPMversion}',
        DefaultUser = '{inDefaultUser}',
        BitlockerReady = '{inBitlockerReady}'
        {inSSHwtypevaldat}
        {inadlastlogonvaldat}
        {ininstalldatevaldat}
        WHERE ID = '{inid}'
        """

        sshw_db_client.execute_sql(sql_command)
    except Exception as e:
        logger.error(f"Error in sshw_upd_pcinfo: {e} SQL: {sql_command}")


def sshw_mac(sshw_db_client, inpcid, inmac):
    try:
        sql_command = f"SELECT pcid FROM mac WHERE macadr = '{inmac}'"
        result = sshw_db_client.execute_sql(sql_command)

        if result:
            sql_command = f"UPDATE mac SET pcid = '{inpcid}' WHERE macadr = '{inmac}'"
        else:
            sql_command = f"INSERT INTO mac (pcid, macadr) VALUES ('{inpcid}', '{inmac}')"

        sshw_db_client.execute_sql(sql_command)

    except Exception as e:
        logger.error(f"Error in sshw_mac: {e} SQL: {sql_command}")


def sshw_ad_only_state(sshw_db_client, inname, inll, inShortsite, inadfqdn, inadgroups):
    try:
        inllval = ", adlastlogon" if inll else ""
        inlldat = f", '{inll}'" if inll else ""
        inllvaldat = f", adlastlogon = '{inll}'" if inll else ""

        inShortsiteval = ", Shortsite" if inShortsite else ""
        inShortsitedat = f", '{inShortsite}'" if inShortsite else ""
        inShortsitevaldat = f", Shortsite = '{inShortsite}'" if inShortsite else ""

        inadfqdnval = ", adfqdn" if inadfqdn else ""
        inadfqdndat = f", '{inadfqdn}'" if inadfqdn else ""
        inadfqdnvaldat = f", adfqdn = '{inadfqdn}'" if inadfqdn else ""

        inadgroupsval = ", adgroups" if inadgroups else ""
        inadgroupsdat = f", '{inadgroups}'" if inadgroups else ""
        inadgroupsvaldat = f", adgroups = '{inadgroups}'" if inadgroups else ""

        sql_command = f"SELECT id FROM pcinfo WHERE name = '{inname}'"
        result = sshw_db_client.execute_sql(sql_command)

        if result:
            sql_command = f"UPDATE pcinfo SET state = '1'{inllvaldat}{inShortsitevaldat}{inadfqdnvaldat}{inadgroupsvaldat} WHERE name = '{inname}'"
        else:
            sql_command = f"INSERT INTO pcinfo (name, state{inllval}{inShortsiteval}{inadfqdnval}{inadgroupsval}) VALUES ('{inname}', '1'{inlldat}{inShortsitedat}{inadfqdndat}{inadgroupsdat})"

        sshw_db_client.execute_sql(sql_command)

    except Exception as e:
        logger.error(f"Error in sshw_ad_only_state: {e} SQL: {sql_command}")


def get_agedate(sshw_db_client, inmodel, inbios):
    try:
        if not inmodel.strip() or inmodel.strip() in ["To be filled by O.E.M", "System Product Name"]:
            inmodel = inbios

        sql_command = f"SELECT agedate FROM modelinfo WHERE model = '{inmodel}'"
        result = sshw_db_client.execute_sql(sql_command)

        if result:
            agedate = result[0]['agedate']
            splitbuf = agedate.split('-')
            return f"{splitbuf[1]}/{splitbuf[0]}/{splitbuf[2]}"
        else:
            return ""

    except Exception as e:
        logger.error(f"Error in get_agedate: {e} SQL: {sql_command}")
        return ""


def get_SSHwtype(sshw_db_client, inmodel, inbios):
    try:
        if not inmodel.strip() or inmodel.strip() in ["To be filled by O.E.M", "System Product Name"]:
            inmodel = inbios

        sql_command = f"SELECT SSHwtype FROM modelinfo WHERE model = '{inmodel}'"
        result = sshw_db_client.execute_sql(sql_command)

        if result:
            return result[0]['SSHwtype']
        else:
            return ""

    except Exception as e:
        logger.error(f"Error in get_SSHwtype: {e} SQL: {sql_command}")
        return ""
