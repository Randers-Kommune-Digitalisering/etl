import io
import pandas as pd
import logging
from utils.config import VOGNPARK_SFTP_DIR

logger = logging.getLogger(__name__)


VOGNPARK_COLUMNS = [
    "Level_1", "Level_2", "Level_3", "Level_4", "Level_5", "Level_6",
    "Art", "Træk", "Drivmiddel", "Reg. nr.", "Mærke", "Model", "Primær bruger",
    "Anvendelse", "Stel nr. "
]


def read_vognpark_excel_from_sftp(sftp_client, remote_path):
    logger.info(f"Reading Excel file from SFTP: {remote_path}")
    with sftp_client.get_connection() as sftp:
        with sftp.open(remote_path, 'rb') as remote_file:
            file_bytes = remote_file.read()
            excel_data = io.BytesIO(file_bytes)
            df = pd.read_excel(excel_data)
    logger.info(f"Successfully read Excel file: {remote_path}")
    df = df[VOGNPARK_COLUMNS]
    return df


def get_latest_vognpark_excel_path(sftp_client, directory=VOGNPARK_SFTP_DIR):
    with sftp_client.get_connection() as sftp:
        files = sftp.listdir_attr(directory)
        excel_files = [f for f in files if f.filename.lower().endswith('.xlsx')]
        if not excel_files:
            logger.error(f"No Excel files found in the directory: {directory}")
            return None
        latest_file = max(excel_files, key=lambda f: f.st_mtime)
        latest_path = directory.rstrip('/') + '/' + latest_file.filename
        logger.info(f"Latest Excel file selected: {latest_path}")
        return latest_path
