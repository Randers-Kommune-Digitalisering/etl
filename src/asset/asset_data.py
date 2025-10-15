import logging
import io
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta
from dateutil.parser import parse
from utils.database_connection import get_asset_db, get_capa_cms_db
from utils.sftp_connection import get_asset_sftp_client
from asset.model import Base
from asset.model import Afdeling, Bruger, Computer
from utils.api_requests import APIClient
from utils.config import (
    ASSET_SFTP_AFDELINGS_EAN_DELTA_FILE_PATH, ASSET_SFTP_DEVICE_FILE_PATH, ASSET_SFTP_COMM2IG_HISTORICAL_FILE_PATH, ASSET_SFTP_EAN_ATEA_FILE_PATH,
    ATEA_API_KEY, ATEA_URL,
)

logger = logging.getLogger(__name__)

atea_client = APIClient(base_url=ATEA_URL, api_key=ATEA_API_KEY, use_subkey=True)
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


def insert_device_license_and_historical_data():
    try:
        sftp_client = get_asset_sftp_client()
        with sftp_client.get_connection() as conn:
            with conn.open(ASSET_SFTP_DEVICE_FILE_PATH, 'r') as file:
                device_license_csv_data = file.read().decode('utf-8')
            with conn.open(ASSET_SFTP_AFDELINGS_EAN_DELTA_FILE_PATH, 'rb') as file:
                afdelings_ean_excel_data = file.read()
            with conn.open(ASSET_SFTP_COMM2IG_HISTORICAL_FILE_PATH, 'r') as file:
                comm2ig_csv_data = file.read().decode('utf-8')
            with conn.open(ASSET_SFTP_EAN_ATEA_FILE_PATH, 'rb') as file:
                atea_excel_data = file.read()

        df_device_license = pd.read_csv(io.StringIO(device_license_csv_data))
        df_device_license.columns = df_device_license.columns.str.strip()
        if 'ComputerName' not in df_device_license.columns:
            logger.error("CSV is missing 'ComputerName' column.")
            return False
        computer_names = [name.strip() for name in df_device_license['ComputerName'].dropna().tolist()]

        df_afdelings_ean = pd.read_excel(io.BytesIO(afdelings_ean_excel_data), dtype=str)
        df_afdelings_ean.columns = df_afdelings_ean.columns.str.strip()
        if 'Institution/afdeling' not in df_afdelings_ean.columns or 'Ean-nummer' not in df_afdelings_ean.columns:
            logger.error("Exel is missing 'Institution/afdeling' or 'Ean-nummer' column.")
            return False

        df_comm2ig = pd.read_csv(io.StringIO(comm2ig_csv_data), dtype=str, sep=',')
        df_comm2ig.columns = df_comm2ig.columns.str.strip()
        required_cols = ['Serienr.', 'Pris pr.stk. i kr. ekskl. moms', 'Fakturadato', 'EAN-nr.']
        missing = [col for col in required_cols if col not in df_comm2ig.columns]
        if missing:
            logger.error(f"Required columns not found in Comm2ig CSV file: {missing}")
            return False

        df_atea = pd.read_excel(io.BytesIO(atea_excel_data), dtype=str)
        df_atea.columns = df_atea.columns.str.strip()
        if 'Nummer' not in df_atea.columns or 'EAN-nr.' not in df_atea.columns:
            logger.error("Atea Excel Data missing 'Nummer' or 'EAN-nr.' column.")
            return False

        atea_data = fetch_atea_data()
        if not atea_data:
            logger.error("No data fetched from Atea API.")
            return False
        billto_map = {str(item.get('BillTo')): item.get('SerialNumber') for item in atea_data if item.get('BillTo') and item.get('SerialNumber')}

        with asset_db_client.get_session() as session:

            computers = session.query(Computer).all()
            name_to_computer = {c.UnitName: c for c in computers if c.UnitName}
            serial_to_computer = {str(c.Serienummer).lstrip('sS').lower(): c for c in computers if c.Serienummer}
            serial_exact_lookup = {str(c.Serienummer): c for c in computers if c.Serienummer}

            # DeviceLicense/AD
            updated_device = 0
            for name in computer_names:
                computer = name_to_computer.get(name)
                if computer:
                    computer.DeviceLicense = True
                    updated_device += 1

            afdelinger = session.query(Afdeling).all()
            afdeling_lookup = {a.Afdeling: a for a in afdelinger if a.Afdeling}

            # AfdelingsEAN/Delta
            updated_ean = 0
            for _, row in df_afdelings_ean.iterrows():
                afdeling = str(row['Institution/afdeling']).strip().lower()
                ean_nummer = str(row['Ean-nummer']).strip()
                if not ean_nummer or ean_nummer.lower() == 'nan':
                    logger.info(f"Skipping update for Department: {afdeling} as Ean-nummer is empty.")
                    continue
                afdeling_obj = afdeling_lookup.get(afdeling)
                if afdeling_obj:
                    afdeling_obj.AfdelingsEAN = ean_nummer
                    updated_ean += 1

            # Comm2ig historisk data
            updated_comm2ig = 0
            for _, row in df_comm2ig.iterrows():
                serial = row['Serienr.']
                serial_norm = str(serial[1:]).lower() if isinstance(serial, str) and serial.startswith('S') else str(serial).lower()
                price = row['Pris pr.stk. i kr. ekskl. moms']
                fakturadato = row['Fakturadato']
                ean_nr = row['EAN-nr.'] if 'EAN-nr.' in row else None

                computer_obj = serial_to_computer.get(serial_norm)
                if computer_obj:
                    try:
                        price_float = float(str(price).replace(',', '.'))
                    except Exception:
                        logger.warning(f"Could not convert price '{price}' for serial '{serial_norm}'")
                        continue
                    computer_obj.Price = price_float
                    computer_obj.OrderDate = fakturadato
                    computer_obj.KøbsEANnr = ean_nr
                    updated_comm2ig += 1

            # Atea KøbsEANnr
            updated_atea = 0
            for _, row in df_atea.iterrows():
                nummer = str(row['Nummer']).strip()
                ean_nr = row['EAN-nr.']
                serial = billto_map.get(nummer)
                computer_obj = serial_exact_lookup.get(serial)
                if serial and computer_obj:
                    computer_obj.KøbsEANnr = ean_nr
                    updated_atea += 1

            session.commit()
            logger.info(f"DeviceLicense updated for {updated_device} computers")
            logger.info(f"AfdelingsEAN updated for {updated_ean} departments")
            logger.info(f"Comm2ig Historical data: Updated Price, Order Date, and KøbsEANnr for {updated_comm2ig} serial numbers.")
            logger.info(f"Atea: KøbsEANnr updated for {updated_atea}")
        return True
    except Exception as e:
        logger.error(f"Error with updating DeviceLicense, AfdelingsEAN, Comm2ig or Atea data: {e}")
        return False
