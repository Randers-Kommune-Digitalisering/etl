from utils.config import (
    TEST_SFTP_HOST,
    TEST_SFTP_USER,
    TEST_SFTP_PASS
)
from utils.stfp import SFTPClient


def get_shared_sftp_client():
    return SFTPClient(TEST_SFTP_HOST, TEST_SFTP_USER, password=TEST_SFTP_PASS)
