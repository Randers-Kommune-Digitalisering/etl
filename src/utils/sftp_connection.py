from utils.config import SENSUM_IT_SFTP_HOST, SENSUM_IT_SFTP_USER, SENSUM_IT_SFTP_PASS
from utils.stfp import SFTPClient


def get_sftp_client():
    return SFTPClient(SENSUM_IT_SFTP_HOST, SENSUM_IT_SFTP_USER, password=SENSUM_IT_SFTP_PASS)
