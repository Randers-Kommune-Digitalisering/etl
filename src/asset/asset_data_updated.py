import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from utils.database_connection import get_asset_db, get_capa_cms_db
from asset.model import Base
from asset.model import Afdeling, Bruger, Computer

logger = logging.getLogger(__name__)

capa_cms_db_client = get_capa_cms_db()
asset_db_client = get_asset_db()


def create_asset_tables():
    try:
        Base.metadata.create_all(asset_db_client.engine)
        logger.info("Created Computer, Bruger, and Afdeling tables if not exists.")
    except Exception as e:
        logger.error(f"Error creating tables: {e}")


def insert_departments_data():
    sql_command = """
    SELECT DISTINCT USI.VALUE AS DEPARTMENT
    FROM USI
    WHERE USI.SECTION = 'General User Inventory'
      AND USI.NAME = 'Department'
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_cms_db_client.execute_sql(sql_command)
        logger.info(f"SQL result: {result}")
        if result:
            session = asset_db_client.get_session()
            inserted = 0
            for row in result:
                department = row[0].lower() if isinstance(row[0], str) else row[0]
                if not session.query(Afdeling).filter_by(Afdeling=department).first():
                    afdeling_obj = Afdeling(Afdeling=department)
                    session.add(afdeling_obj)
                    inserted += 1
            session.commit()
            logger.info(f"Inserted {inserted} unique departments into Afdeling table.")
            return True
        else:
            logger.error("No Department data found.")
            return False
    except Exception as e:
        logger.error(f"Error inserting departments into Afdeling table: {e}")
        return False


def insert_users_data():
    sql_command = """
    WITH PrimaryUsers AS (
        SELECT DISTINCT UNIT.UNITID, UNIT.NAME AS PC_UNIT_NAME,
               REPLACE(REPLACE(LGI.VALUE, '@LAKSEN04', ''), '@RANDERS.DK', '') AS PRIMARY_USER
        FROM UNIT
        JOIN LGI ON UNIT.UNITID = LGI.UNITID
        WHERE LGI.SECTION = 'Current Logon' AND LGI.NAME = 'User Name'
    )
    SELECT DISTINCT DU.PRIMARY_USER, USI.VALUE AS FULLNAME, LOWER(USI2.VALUE) AS DEPARTMENT
    FROM PrimaryUsers DU
    JOIN UNIT ON UNIT.NAME = DU.PRIMARY_USER
    JOIN USI ON UNIT.UNITID = USI.UNITID AND USI.SECTION = 'General User Inventory' AND USI.NAME = 'Full Name'
    JOIN USI USI2 ON UNIT.UNITID = USI2.UNITID AND USI2.SECTION = 'General User Inventory' AND USI2.NAME = 'Department'
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_cms_db_client.execute_sql(sql_command)
        logger.info(f"SQL result: {result}")
        if not result:
            logger.error("No user data found.")
            return False

        session = asset_db_client.get_session()
        afdelinger = {a.Afdeling: a.AfdelingID for a in session.query(Afdeling).all()}
        eksisterende_brugere = set(b.PrimaryUser for b in session.query(Bruger.PrimaryUser).all())

        to_insert = []
        for primary_user, fullname, department in result:
            afdeling_id = afdelinger.get(department.lower() if isinstance(department, str) else department)
            if afdeling_id and primary_user not in eksisterende_brugere:
                to_insert.append({
                    "PrimaryFullName": fullname,
                    "PrimaryUser": primary_user,
                    "AfdelingID": afdeling_id
                })

        if to_insert:
            session.bulk_insert_mappings(Bruger, to_insert)
            session.commit()
            logger.info(f"Inserted {len(to_insert)} unique users into Bruger table.")
        else:
            logger.info("No new users to insert.")
        return True

    except Exception as e:
        logger.error(f"Error inserting users into Bruger table: {e}")
        return False


def insert_computers_data():
    sql_command = """
    SELECT
        U.NAME AS UnitName,
        INV.VALUE AS Producent,
        CSI.VALUE AS Model,
        DEVICETYPE.HWNAME AS Enhedstype,
        U.SERIALNUMBER AS Serienummer,
        FORMAT(DATEADD(HOUR, 1, DATEADD(SECOND, TRY_CAST(U.LASTONLINE AS BIGINT), '1970-01-01')), 'yyyy-MM-ddTHH:mm:ss.ff') AS SidsteLoginDato,
        FORMAT(DATEADD(SECOND, TRY_CAST(INV2.VALUE AS BIGINT), '1970-01-01'), 'yyyy-MM-ddTHH:mm:ss.ff') AS SidsteRul,
        REPLACE(REPLACE(LGI.VALUE, '@LAKSEN04', ''), '@RANDERS.DK', '') AS PrimaryUser,
        BLK.VALUE AS BitlockerKode,
        BLS.VALUE AS BitlockerStatus,
        BLE.VALUE AS BitlockerKrypteringProcent,
        OSINV.VALUE AS OSVersion
    FROM UNIT U
    LEFT JOIN INV ON U.UNITID = INV.UNITID
        AND INV.SECTION = 'System'
        AND INV.NAME = 'Manufacturer'
    LEFT JOIN CSI ON U.UNITID = CSI.UNITID
        AND CSI.SECTION = 'Randers Kommune'
        AND CSI.NAME = 'WSName'
    LEFT JOIN DEVICETYPE ON U.DEVICETYPEID = DEVICETYPE.ID
    LEFT JOIN INV INV2 ON U.UNITID = INV2.UNITID
        AND INV2.SECTION = 'Operating System'
        AND INV2.NAME = 'InstallDate'
    LEFT JOIN LGI ON U.UNITID = LGI.UNITID
        AND LGI.SECTION = 'Current Logon'
        AND LGI.NAME = 'User Name'
    LEFT JOIN CSI BLK ON U.UNITID = BLK.UNITID
        AND BLK.SECTION = 'CapaServices | CapaBitLocker'
        AND (
            BLK.NAME = 'Recovery Password C: #1 Password'
            OR BLK.NAME = 'Recovery Password D: #1 Password'
            OR BLK.NAME = 'Recovery Password E: #1 Password'
        )
    LEFT JOIN CSI BLS ON U.UNITID = BLS.UNITID
        AND BLS.SECTION = 'CapaServices | CapaBitLocker'
        AND BLS.NAME = 'Protection Status C:'
    LEFT JOIN CSI BLE ON U.UNITID = BLE.UNITID
        AND BLE.SECTION = 'CapaServices | CapaBitLocker'
        AND BLE.NAME = 'Encryption Status C:'
    LEFT JOIN INV OSINV ON U.UNITID = OSINV.UNITID
        AND OSINV.SECTION = 'Operating System'
        AND OSINV.NAME = 'System'
    WHERE U.SERIALNUMBER IS NOT NULL
      AND U.TYPE = 1
    """
    logger.info(f"Executing SQL command: {sql_command}")

    try:
        result = capa_cms_db_client.execute_sql(sql_command)
        logger.info(f"SQL result: {result}")
        if result:
            session = asset_db_client.get_session()
            inserted = 0
            updated = 0
            six_months_ago = datetime.now() - relativedelta(months=6)
            for row in result:
                (
                    unit_name, producent, model, enhedstype, serienummer, sidste_login_dato, sidste_rul, primary_user,
                    bitlocker_kode, bitlocker_status, bitlocker_kryptering, os_version
                ) = row
                bruger_obj = session.query(Bruger).filter_by(PrimaryUser=primary_user).first()
                bruger_id = bruger_obj.BrugerID if bruger_obj else None

                drift_status = False
                if sidste_login_dato:
                    try:
                        last_login = parse(str(sidste_login_dato))
                        if last_login >= six_months_ago:
                            drift_status = True
                    except Exception:
                        logger.error(f"Could not parse SidsteLoginDato: {sidste_login_dato} for {unit_name}")

                computer = session.query(Computer).filter_by(UnitName=unit_name).first()
                if computer:
                    computer.Producent = producent
                    computer.Model = model
                    computer.Enhedstype = enhedstype
                    computer.Serienummer = serienummer
                    computer.SidsteLoginDato = sidste_login_dato
                    computer.SidsteRul = sidste_rul
                    computer.BrugerID = bruger_id
                    computer.BitlockerKode = bitlocker_kode
                    computer.BitlockerStatus = bitlocker_status
                    computer.BitlockerKrypteringProcent = bitlocker_kryptering
                    computer.OSVersion = os_version
                    computer.Drift = drift_status
                    updated += 1
                else:
                    computer = Computer(
                        UnitName=unit_name,
                        Producent=producent,
                        Model=model,
                        Enhedstype=enhedstype,
                        Serienummer=serienummer,
                        SidsteLoginDato=sidste_login_dato,
                        SidsteRul=sidste_rul,
                        BrugerID=bruger_id,
                        BitlockerKode=bitlocker_kode,
                        BitlockerStatus=bitlocker_status,
                        BitlockerKrypteringProcent=bitlocker_kryptering,
                        OSVersion=os_version,
                        Drift=drift_status
                    )
                    session.add(computer)
                    inserted += 1
            session.commit()
            logger.info(f"Inserted {inserted}, updated {updated} computers in Computer table.")
            return True
        else:
            logger.error("No computer data found.")
            return False
    except Exception as e:
        logger.error(f"Error inserting computers into Computer table: {e}")
        return False
