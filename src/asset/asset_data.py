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
    logger.info(f"Executing Department SQL command: {sql_command}")

    try:
        result = capa_cms_db_client.execute_sql(sql_command)
        logger.debug(f"Department SQL result: {result}")
        if result:
            with asset_db_client.get_session() as session:
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
    logger.info(f"Executing User SQL command: {sql_command}")

    try:
        result = capa_cms_db_client.execute_sql(sql_command)
        logger.debug(f"User SQL result: {result}")
        if not result:
            logger.error("No user data found.")
            return False

        with asset_db_client.get_session() as session:
            departments = {d.Afdeling: d for d in session.query(Afdeling).all()}
            existing_users = {u.PrimaryUser: u for u in session.query(Bruger).all()}

            user_data = {}
            for primary_user, fullname, department in result:
                department = department.lower() if isinstance(department, str) else department
                if primary_user not in user_data:
                    user_data[primary_user] = {"fullname": fullname, "departments": set()}
                user_data[primary_user]["departments"].add(department)

            inserted = 0
            for primary_user, data in user_data.items():

                user = existing_users.get(primary_user)
                if not user:
                    user = Bruger(PrimaryFullName=data["fullname"], PrimaryUser=primary_user)
                    session.add(user)
                    session.flush()
                    inserted += 1

                for dept in data["departments"]:
                    department_obj = departments.get(dept)
                    if department_obj and department_obj not in user.afdelinger:
                        user.afdelinger.append(department_obj)

            session.commit()
            logger.info(f"Inserted/updated {inserted} users and linked departments.")
        return True

    except Exception as e:
        logger.error(f"Error inserting users into User table: {e}")
        return False


def insert_computers_data():
    sql_command = """
    SELECT
        U.NAME AS UnitName,
        INV.VALUE AS Producent,
        CSI.VALUE AS Model,
        DEVICETYPE.HWNAME AS Enhedstype,
        U.SERIALNUMBER AS Serienummer,
        DATEADD(HOUR, 1, DATEADD(SECOND, TRY_CAST(U.LASTONLINE AS BIGINT), '1970-01-01')) AS SidsteLoginDato,
        DATEADD(SECOND, TRY_CAST(INV2.VALUE AS BIGINT), '1970-01-01') AS SidsteRul,
        REPLACE(REPLACE(LGI.VALUE, '@LAKSEN04', ''), '@RANDERS.DK', '') AS PrimaryUser,
        BLK.VALUE AS BitlockerKode,
        BLS.VALUE AS BitlockerStatus,
        BLE.VALUE AS BitlockerKrypteringProcent,
        OSINV.VALUE AS OSVersion,
        (
            SELECT STRING_AGG(MACINV.VALUE, ',')
            FROM INV MACINV
            WHERE MACINV.UNITID = U.UNITID
              AND MACINV.SECTION = 'Network Adapter'
              AND MACINV.NAME LIKE 'Device #% MAC Address'
              AND NOT EXISTS (
                  SELECT 1
                  FROM INV IPLAN
                  WHERE IPLAN.UNITID = MACINV.UNITID
                    AND IPLAN.SECTION = 'Network Configuration'
                    AND IPLAN.NAME LIKE 'Device #% IP address'
                    AND SUBSTRING(MACINV.NAME, 9, CHARINDEX(' ', MACINV.NAME, 9) - 9) = SUBSTRING(IPLAN.NAME, 9, CHARINDEX(' ', IPLAN.NAME, 9) - 9)
                    AND (
                        IPLAN.VALUE LIKE '10.129.%'
                        OR IPLAN.VALUE LIKE '10.146.%'
                        OR IPLAN.VALUE LIKE '10.161.%'
                        OR IPLAN.VALUE LIKE '10.177.%'
                    )
              )
        ) AS MACAdresse,
        (
            SELECT STRING_AGG(MACINV.VALUE, ',')
            FROM INV MACINV
            JOIN INV IPINV ON MACINV.UNITID = IPINV.UNITID
                AND MACINV.SECTION = 'Network Adapter'
                AND IPINV.SECTION = 'Network Configuration'
                AND MACINV.NAME LIKE 'Device #% MAC Address'
                AND IPINV.NAME LIKE 'Device #% IP address'
                AND SUBSTRING(MACINV.NAME, 9, CHARINDEX(' ', MACINV.NAME, 9) - 9) = SUBSTRING(IPINV.NAME, 9, CHARINDEX(' ', IPINV.NAME, 9) - 9)
                AND (
                    IPINV.VALUE LIKE '10.129.%'
                    OR IPINV.VALUE LIKE '10.146.%'
                    OR IPINV.VALUE LIKE '10.161.%'
                    OR IPINV.VALUE LIKE '10.177.%'
                )
            WHERE MACINV.UNITID = U.UNITID
              AND MACINV.SECTION = 'Network Adapter'
              AND MACINV.NAME LIKE 'Device #% MAC Address'
        ) AS LanMACAdresse
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
    logger.info(f"Executing Computer SQL command: {sql_command}")

    try:
        result = capa_cms_db_client.execute_sql(sql_command)
        logger.debug(f"Computer SQL result: {result}")
        if result:
            inserted = 0
            updated = 0
            six_months_ago = datetime.now() - relativedelta(months=6)
            with asset_db_client.get_session() as session:
                for row in result:
                    (
                        unit_name, producent, model, enhedstype, serienummer, sidste_login_dato, sidste_rul, primary_user,
                        bitlocker_kode, bitlocker_status, bitlocker_kryptering, os_version, mac_adresse, lan_mac_adresse
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
                        computer.MACAdresse = mac_adresse
                        computer.LanMACAdresse = lan_mac_adresse
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
                            Drift=drift_status,
                            MACAdresse=mac_adresse,
                            LanMACAdresse=lan_mac_adresse
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
