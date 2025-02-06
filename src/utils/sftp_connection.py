from utils.config import ASSET_SFTP_HOST, ASSET_SFTP_USER, ASSET_SFTP_PASS
from utils.stfp import SFTPClient


def get_sftp_client():
    return SFTPClient(ASSET_SFTP_HOST, ASSET_SFTP_USER, password=ASSET_SFTP_PASS)


def download_csv_from_sftp(sftp_file_path):
    sftp_client = get_sftp_client()
    with sftp_client.get_connection() as conn:
        with conn.open(sftp_file_path, 'r') as file:
            csv_data = file.read().decode('utf-8')
    return csv_data
