from utils.config import (
    SENSUM_IT_SFTP_HOST,
    SENSUM_IT_SFTP_USER,
    SENSUM_IT_SFTP_PASS,
    ASSET_SFTP_HOST,
    ASSET_SFTP_USER,
    ASSET_SFTP_PASS,
    TEST_SFTP_HOST,
    TEST_SFTP_USER,
    TEST_SFTP_PASS
)
from utils.stfp import SFTPClient


def get_sensum_sftp_client():
    return SFTPClient(SENSUM_IT_SFTP_HOST, SENSUM_IT_SFTP_USER, password=SENSUM_IT_SFTP_PASS)


def get_asset_sftp_client():
    return SFTPClient(ASSET_SFTP_HOST, ASSET_SFTP_USER, password=ASSET_SFTP_PASS)


def get_shared_sftp_client():
    return SFTPClient(TEST_SFTP_HOST, TEST_SFTP_USER, password=TEST_SFTP_PASS)
