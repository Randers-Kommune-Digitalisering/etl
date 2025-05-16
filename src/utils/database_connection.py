from utils.database_client import DatabaseClient
from utils.config import (
    SENSUM_DB_DATABASE, SENSUM_DB_USER, SENSUM_DB_PASS, SENSUM_DB_HOST, SENSUM_DB_PORT,
    POSTGRES_DB_USER, POSTGRES_DB_PASS, POSTGRES_DB_HOST, POSTGRES_DB_DATABASE, POSTGRES_DB_PORT,
    ZYLINC_POSTGRES_DB_DATABASE, ZYLINC_POSTGRES_DB_USER, ZYLINC_POSTGRES_DB_PASS, ZYLINC_POSTGRES_DB_HOST, ZYLINC_POSTGRES_DB_PORT,
    ASSET_DB_DATABASE, ASSET_DB_USER, ASSET_DB_PASS, ASSET_DB_HOST, ASSET_DB_PORT,
    CAPA_CMS_DB_DATABASE, CAPA_CMS_DB_USER, CAPA_CMS_DB_PASS, CAPA_CMS_DB_HOST, CAPA_CMS_DB_PORT,
    BYGGESAGER_POSTGRES_DB_DATABASE, BYGGESAGER_POSTGRES_DB_USER, BYGGESAGER_POSTGRES_DB_PASS, BYGGESAGER_POSTGRES_DB_HOST, BYGGESAGER_POSTGRES_DB_PORT,
    JOBINDSATS_POSTGRES_DB_HOST, JOBINDSATS_POSTGRES_DB_USER, JOBINDSATS_POSTGRES_DB_PASS, JOBINDSATS_POSTGRES_DB_DATABASE,
    JOBINDSATS_POSTGRES_DB_PORT
)


def get_db_client():
    return DatabaseClient(
        db_type='postgresql',
        database=SENSUM_DB_DATABASE,
        username=SENSUM_DB_USER,
        password=SENSUM_DB_PASS,
        host=SENSUM_DB_HOST,
        port=SENSUM_DB_PORT
    )


def get_db_frontdesk():
    return DatabaseClient(
        db_type='postgresql',
        database=POSTGRES_DB_DATABASE,
        username=POSTGRES_DB_USER,
        password=POSTGRES_DB_PASS,
        host=POSTGRES_DB_HOST,
        port=POSTGRES_DB_PORT
    )


def get_db_zylinc():
    return DatabaseClient(
        db_type='postgresql',
        database=ZYLINC_POSTGRES_DB_DATABASE,
        username=ZYLINC_POSTGRES_DB_USER,
        password=ZYLINC_POSTGRES_DB_PASS,
        host=ZYLINC_POSTGRES_DB_HOST,
        port=ZYLINC_POSTGRES_DB_PORT
    )


def get_capa_cms_db():
    return DatabaseClient(
        db_type='mssql',
        database=CAPA_CMS_DB_DATABASE,
        username=CAPA_CMS_DB_USER,
        password=CAPA_CMS_DB_PASS,
        host=CAPA_CMS_DB_HOST,
        port=CAPA_CMS_DB_PORT
    )


def get_asset_db():
    return DatabaseClient(
        db_type='postgresql',
        database=ASSET_DB_DATABASE,
        username=ASSET_DB_USER,
        password=ASSET_DB_PASS,
        host=ASSET_DB_HOST,
        port=ASSET_DB_PORT
    )


def get_byggesager_db():
    return DatabaseClient(
        db_type='postgresql',
        database=BYGGESAGER_POSTGRES_DB_DATABASE,
        username=BYGGESAGER_POSTGRES_DB_USER,
        password=BYGGESAGER_POSTGRES_DB_PASS,
        host=BYGGESAGER_POSTGRES_DB_HOST,
        port=BYGGESAGER_POSTGRES_DB_PORT
    )


def get_jobindsats_db():
    return DatabaseClient(
        db_type='postgresql',
        database=JOBINDSATS_POSTGRES_DB_DATABASE,
        username=JOBINDSATS_POSTGRES_DB_USER,
        password=JOBINDSATS_POSTGRES_DB_PASS,
        host=JOBINDSATS_POSTGRES_DB_HOST,
        port=JOBINDSATS_POSTGRES_DB_PORT
    )