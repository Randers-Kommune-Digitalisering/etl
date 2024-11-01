import os
import io
import fnmatch
import logging
import pandas as pd
from datetime import datetime, timedelta
from utils.config import SENSUM_IT_SFTP_HOST, SENSUM_IT_SFTP_USER, SENSUM_IT_SFTP_PASS, SENSUM_IT_SFTP_REMOTE_DIR
from utils.stfp import SFTPClient
from custom_data_connector import post_data_to_custom_data_connector

logger = logging.getLogger(__name__)


def job():
    sftp_client = SFTPClient(SENSUM_IT_SFTP_HOST, SENSUM_IT_SFTP_USER, password=SENSUM_IT_SFTP_PASS)
    conn = sftp_client.get_connection()
    sager_files = [f for f in conn.listdir(SENSUM_IT_SFTP_REMOTE_DIR) if fnmatch.fnmatch(f, 'Sager_*.csv')]
    leverandor_files = [f for f in conn.listdir(SENSUM_IT_SFTP_REMOTE_DIR) if fnmatch.fnmatch(f, 'Leverandør_*.csv')]
    indsatser_files = [f for f in conn.listdir(SENSUM_IT_SFTP_REMOTE_DIR) if fnmatch.fnmatch(f, 'Indsatser_*.csv')]
    borger_files = [f for f in conn.listdir(SENSUM_IT_SFTP_REMOTE_DIR) if fnmatch.fnmatch(f, 'Borger_*.csv')]

    if sager_files and conn:
        handle_sager_files(sager_files, conn)

    if leverandor_files and conn:
        handle_leverandor_files(leverandor_files, conn)

    if indsatser_files and conn:
        handle_Indsatser_files(indsatser_files, conn)

    if borger_files and conn:
        handle_borger_files(borger_files, conn)

    return False


def handle_sager_files(files, connection):
    logger.info('Handling Sensum Sager files')

    try:

        latest_file = max(files, key=lambda f: connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, f)).st_mtime)
        logger.info(f"Latest file: {latest_file}")

        last_modified_time = connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, latest_file)).st_mtime
        date = datetime.fromtimestamp(last_modified_time)

        max_date = date - timedelta(days=1)
        min_date = datetime(date.year - 2, date.month, 1)

        logger.info(f'Data periode: {min_date} - {max_date}')

        files = [f for f in files if datetime.fromtimestamp(connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, f)).st_mtime) >= min_date]

        df_list = []

        min_date = pd.to_datetime(min_date)

        for filename in files:
            with connection.open(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, filename).replace("\\", "/")) as f:
                needed_cols = [
                    'SagId', 'SagNavn', 'SagType', 'SagModel', 'BorgerId', 'AfdelingId',
                    'AfdelingNavn', 'PrimærAnsvarlig', 'AlternativSagsbehandler',
                    'AlternativTeam', 'ForventetParagraf', 'Status', 'Akut', 'HandleKommune',
                    'BetalingsKommune', 'SagsbehandlerBetalingsKommune', 'HenvendelsesDato',
                    'AnsøgningModtagetDato', 'SlutDato', 'AfslutningsÅrsag', 'Facet', 'JournalKode'
                ]

                df = pd.read_csv(f, sep=";", header=0, decimal=",", usecols=needed_cols)
                # Filling missing values in columns with the first row's value
                if pd.isna(df.at[0, 'SlutDato']):
                    df.at[0, 'SlutDato'] = "Missing Value"
                if pd.isna(df.at[0, 'AlternativTeam']):
                    df.at[0, 'AlternativTeam'] = "Missing Value"
                if pd.isna(df.at[0, 'AlternativSagsbehandler']):
                    df.at[0, 'AlternativSagsbehandler'] = "Missing Value"
                if pd.isna(df.at[0, 'SagsbehandlerBetalingsKommune']):
                    df.at[0, 'SagsbehandlerBetalingsKommune'] = "Missing Value"
                if pd.isna(df.at[0, 'AfslutningsÅrsag']):
                    df.at[0, 'AfslutningsÅrsag'] = "Missing Value"

                df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)

        file = io.BytesIO(df.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "Sager" + ".csv"

        if post_data_to_custom_data_connector(filename, file):
            logger.info("Successfully updated Sager")
            return True
        else:
            logger.error("Failed to update Sager")
            return False

        # if df_list:
        #     df = pd.concat(df_list, ignore_index=True)
        #     df_to_csv(df, 'Sager')
        #     return df
        # else:
        #     logger.error("No data frames to concatenate.")
        #     return None

    except Exception as e:
        logger.error(f"Error handling files: {e}")
        return None


def handle_leverandor_files(files, connection):
    logger.info('Handling Sensum Leverandør files')

    try:
        latest_file = max(files, key=lambda f: connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, f)).st_mtime)
        logger.info(f"Latest file: {latest_file}")

        last_modified_time = connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, latest_file)).st_mtime
        date = datetime.fromtimestamp(last_modified_time)

        max_date = date - timedelta(days=1)
        min_date = datetime(date.year - 2, date.month, 1)

        logger.info(f"Data periode: {min_date} - {max_date}")

        files = [f for f in files if datetime.fromtimestamp(connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, f)).st_mtime) >= min_date]

        df_list = []

        min_date = pd.to_datetime(min_date)

        for filename in files:
            with connection.open(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, filename).replace("\\", "/")) as f:
                needed_cols = [
                    'LeverandørId', 'LeverandørNavn', 'LeverandørGruppe', 'LeverandørAdresse', 'LeverandørPostNr', 'LeverandørBy',
                    'LeverandørLand', 'LeverandørTelefon', 'LeverandørMobil',
                    'LeverandørFax', 'LeverandørEmail', 'LeverandørNummer', 'LeverandørCVR', 'LeverandørEjerskab',
                    'LeverandørEgetAfMyndighed', 'LeverandørSidstÆndret', 'BostedSystemId',
                    'Aktiv'
                ]

                df = pd.read_csv(f, sep=";", header=0, decimal=",", usecols=needed_cols)

                if pd.isna(df.at[0, 'LeverandørTelefon']):
                    df.at[0, 'LeverandørTelefon'] = "Missing Value"
                if pd.isna(df.at[0, 'LeverandørMobil']):
                    df.at[0, 'LeverandørMobil'] = "Missing Value"
                if pd.isna(df.at[0, 'LeverandørFax']):
                    df.at[0, 'LeverandørFax'] = 'Missing Value'
                if pd.isna(df.at[0, 'LeverandørEmail']):
                    df.at[0, 'LeverandørEmail'] = 'Missing Value'
                if pd.isna(df.at[0, 'LeverandørNummer']):
                    df.at[0, 'LeverandørNummer'] = 'Missing Value'
                if pd.isna(df.at[0, 'LeverandørCVR']):
                    df.at[0, 'LeverandørCVR'] = 'Missing Value'
                if pd.isna(df.at[0, 'BostedSystemId']):
                    df.at[0, 'BostedSystemId'] = 'Missing Value'

                df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)

        file = io.BytesIO(df.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "Leverandor" + ".csv"

        if post_data_to_custom_data_connector(filename, file):
            logger.info("Successfully updated Leverandør")
            return True
        else:
            logger.error("Failed to update Leverandør")
            return False

    except Exception as e:
        logger.error(f"Error handling Leverandør files: {e}")


def handle_Indsatser_files(files, connection):
    logger.info('Handling Sensum Indsatser files')

    try:
        latest_file = max(files, key=lambda f: connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, f)).st_mtime)
        logger.info(f"Latest file: {latest_file}")

        last_modified_time = connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, latest_file)).st_mtime
        date = datetime.fromtimestamp(last_modified_time)

        max_date = date - timedelta(days=1)
        min_date = datetime(date.year -2, date.month, 1)

        logger.info(f"Data periode: {min_date} - {max_date}")

        files = [f for f in files if datetime.fromtimestamp(connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, f)).st_mtime) >= min_date]

        df_list = []

        min_date = pd.to_datetime(min_date)

        for filename in files:
            with connection.open(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, filename).replace("\\", "/")) as f:                
                needed_cols = [
                    'Tilbud', 'LeverandørId', 'LeverandørIndsatsId', 'LeverandørNavn', 'LeverandørGruppeId', 'LeverandørGruppeNavn',
                    'IndsatsStatus', 'IndsatsGodkendelsesDato', 'IndsatsStartDato', 'IndsatsSlutDato', 'Primær målgruppe',
                    'Sekundær målgruppe', 'IndsatsKreditKonto', 'IndsatsEgenbetalingsKonto', 'IndsatskontoNummer', 'StatsRefusionsKontoNummer',
                    'MomsRefusionsKontoNummer', 'OverheadKontoNummer', 'EgenBetalingsKontoNummer',
                    'IndsatsId navn', 'Afgørelse'
                ]

                df = pd.read_csv(f, sep=";", header=0, decimal=",", usecols=needed_cols)
                if pd.isna(df.at[0, 'Sekundær målgruppe']):
                    df.at[0, 'Sekundær målgruppe'] = "Missing Value"
                if pd.isna(df.at[0, 'IndsatsKreditKonto']):
                    df.at[0, 'IndsatsKreditKonto'] = "Missing Value"
                if pd.isna(df.at[0, 'IndsatsEgenbetalingsKonto']):
                    df.at[0, 'IndsatsEgenbetalingsKonto'] = "Missing Value"
                if pd.isna(df.at[0, 'IndsatskontoNummer']):
                    df.at[0, 'IndsatskontoNummer'] = "Missing Value"
                if pd.isna(df.at[0, 'StatsRefusionsKontoNummer']):
                    df.at[0, 'StatsRefusionsKontoNummer'] = "Missing Value"
                if pd.isna(df.at[0, 'MomsRefusionsKontoNummer']):
                    df.at[0, 'MomsRefusionsKontoNummer'] = "Missing Value"
                if pd.isna(df.at[0, 'OverheadKontoNummer']):
                    df.at[0, 'OverheadKontoNummer'] = "Missing Value"
                if pd.isna(df.at[0, 'EgenBetalingsKontoNummer']):
                    df.at[0, 'EgenBetalingsKontoNummer'] = "Missing Value"
                if pd.isna(df.at[0, 'Afgørelse']):
                    df.at[0, 'Afgørelse'] = "Missing Value"

                df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)

        file = io.BytesIO(df.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "Indsatser" + ".csv"

        if post_data_to_custom_data_connector(filename, file):
            logger.info("Successfully updated Indsatser")
            return True
        else:
            logger.error("Failed to update Indsatser")
            return False

    except Exception as e:
        logger.error(f"Error handling Indsatser files: {e}")


def handle_borger_files(files, connection):
    logger.info('"Handling Sensum Borger files')

    try:
        latest_file = max(files, key=lambda f: connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, f)).st_mtime)
        logger.info(f"Latest file: {latest_file}")

        last_modified_time = connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, latest_file)).st_mtime
        date = datetime.fromtimestamp(last_modified_time)

        max_date = date - timedelta(days=1)
        min_date = datetime(date.year - 2, date.month, 1)

        logger.info(f"Data periode: {min_date} - {max_date}")

        files = [f for f in files if datetime.fromtimestamp(connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, f)).st_mtime) >= min_date]

        df_list = []

        min_date = pd.to_datetime(min_date)

        for filename in files:
            with connection.open(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, filename).replace("\\", "/")) as f:
                needed_cols = [
                    'BorgerId', 'CPR', 'UdlændingeId', 'MidlertidigtCpr', 'Fornavn', 'Efternavn', 'Initialer',
                    'PrimærAdresse', 'PrimærPostNr', 'PrimærBy', 'PrimærLand', 'Telefon', 'Mobil', 'Email',
                    'KontaktPerson', 'OprettetDato', 'Diagnose', 'Kommentar', 'OpholdsKommuneNr',
                    'OpholdsKommune', 'BorgerEncodeId'
                ]

                df = pd.read_csv(f, sep=";", header=0, decimal=",", usecols=needed_cols)
                if pd.isna(df.at[0, 'CPR']):
                    df.at[0, 'CPR'] = "Missing Value"
                if pd.isna(df.at[0, 'UdlændingeId']):
                    df.at[0, 'UdlændingeId'] = "Missing Value"
                if pd.isna(df.at[0, 'MidlertidigtCpr']):
                    df.at[0, 'MidlertidigtCpr'] = "Missing Value"
                if pd.isna(df.at[0, 'PrimærLand']):
                    df.at[0, 'PrimærLand'] = "Missing Value"
                if pd.isna(df.at[0, 'Telefon']):
                    df.at[0, 'Telefon'] = "Missing Value"
                if pd.isna(df.at[0, 'Mobil']):
                    df.at[0, 'Mobil'] = "Missing Value"
                if pd.isna(df.at[0, 'Email']):
                    df.at[0, 'Email'] = "Missing Value"
                if pd.isna(df.at[0, 'Diagnose']):
                    df.at[0, 'Diagnose'] = "Missing Value"
                if pd.isna(df.at[0, 'OpholdsKommuneNr']):
                    df.at[0, 'OpholdsKommuneNr'] = "Missing Value"
                if pd.isna(df.at[0, 'OpholdsKommune']):
                    df.at[0, 'OpholdsKommune'] = "Missing Value"

                df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)

        file = io.BytesIO(df.to_csv(index=False, sep=';').encode('utf-8'))
        filename = "Borger" + ".csv"

        if post_data_to_custom_data_connector(filename, file):
            logger.info("Successfully updated Borger")
            return True
        else:
            logger.error("Failed to update borger")

    except Exception as e:
        logger.error(f"Error handling Borger files: {e}")
        return None


def df_to_csv(df, filename):  # For testing locally purposes
    try:
        df.to_csv(f"{filename}.csv", index=False)
        logger.info(f"DataFrame saved to {filename}.csv")
    except Exception as e:
        logger.error(f"Error saving DataFrame to CSV: {e}")
