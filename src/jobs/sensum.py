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
    indsatser_files = [f for f in conn.listdir(SENSUM_IT_SFTP_REMOTE_DIR) if fnmatch.fnmatch(f, 'Indsatser_*.csv')]
    leverandor_files = [f for f in conn.listdir(SENSUM_IT_SFTP_REMOTE_DIR) if fnmatch.fnmatch(f, 'Leverandør_*.csv')]
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
                    'SagId', 'BorgerId', 'SagNavn', 'SagType', 'SagModel', 'AfdelingId',
                    'AfdelingNavn', 'PrimærAnsvarlig', 'AlternativSagsbehandler',
                    'AlternativTeam', 'ForventetParagraf', 'Status', 'Akut', 'HandleKommune',
                    'BetalingsKommune', 'SagsbehandlerBetalingsKommune', 'HenvendelsesDato',
                    'AnsøgningModtagetDato', 'SlutDato', 'AfslutningsÅrsag', 'Facet', 'JournalKode'
                ]

                df = pd.read_csv(f, sep=";", header=0, decimal=",", usecols=needed_cols)

                df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)
        return df

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

                df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)
        return df

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
        min_date = datetime(date.year - 2, date.month, 1)

        logger.info(f"Data periode: {min_date} - {max_date}")

        files = [f for f in files if datetime.fromtimestamp(connection.stat(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, f)).st_mtime) >= min_date]

        df_list = []

        min_date = pd.to_datetime(min_date)

        for filename in files:
            with connection.open(os.path.join(SENSUM_IT_SFTP_REMOTE_DIR, filename).replace("\\", "/")) as f:
                needed_cols = [
                    'IndsatsId', 'SagId', 'Indsats', 'VigtigeNotater', 'IndsatsParagraf',
                    'Tilbud', 'LeverandørId', 'LeverandørIndsatsId', 'LeverandørNavn', 'LeverandørGruppeId', 'LeverandørGruppeNavn',
                    'IndsatsStatus', 'IndsatsGodkendelsesDato', 'IndsatsStartDato', 'IndsatsSlutDato', 'Primær målgruppe',
                    'Sekundær målgruppe', 'IndsatsKreditKonto', 'IndsatsEgenbetalingsKonto', 'IndsatskontoNummer', 'StatsRefusionsKontoNummer',
                    'MomsRefusionsKontoNummer', 'OverheadKontoNummer', 'EgenBetalingsKontoNummer',
                    'IndsatsId navn', 'Afgørelse'
                ]

                df = pd.read_csv(f, sep=";", header=0, decimal=",", usecols=needed_cols)

                df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)
        return df

    except Exception as e:
        logger.error(f"Error handling Indsatser files: {e}")
        return None


def handle_borger_files(files, connection):
    logger.info('Handling Sensum Borger files')

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
                    'BorgerId', 'CPR', 'Fornavn', 'Efternavn', 'Initialer',
                    'PrimærAdresse', 'PrimærPostNr', 'PrimærBy', 'PrimærLand', 'Telefon', 'Mobil', 'Email',
                    'KontaktPerson', 'OprettetDato', 'Diagnose', 'Kommentar', 'OpholdsKommuneNr',
                    'OpholdsKommune', 'BorgerEncodeId'
                ]

                df = pd.read_csv(f, sep=";", header=0, decimal=",", usecols=needed_cols)

                df_list.append(df)

        df = pd.concat(df_list, ignore_index=True)
        return df

    except Exception as e:
        logger.error(f"Error handling Borger files: {e}")
        return None


def df_to_csv(df, filename):  # For testing locally purposes
    try:
        df.to_csv(f"{filename}.csv", index=False)
        logger.info(f"DataFrame saved to {filename}.csv")
    except Exception as e:
        logger.error(f"Error saving DataFrame to CSV: {e}")
