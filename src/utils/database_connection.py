from utils.database_client import DatabaseClient
from utils.config import (
    SENSUM_DB_DATABASE, SENSUM_DB_USER, SENSUM_DB_PASS, SENSUM_DB_HOST, SENSUM_DB_PORT,
    POSTGRES_DB_USER, POSTGRES_DB_PASS, POSTGRES_DB_HOST, POSTGRES_DB_DATABASE, POSTGRES_DB_PORT,
    ZYLINC_POSTGRES_DB_DATABASE, ZYLINC_POSTGRES_DB_USER, ZYLINC_POSTGRES_DB_PASS, ZYLINC_POSTGRES_DB_HOST, ZYLINC_POSTGRES_DB_PORT,
    ASSET_DB_DATABASE, ASSET_DB_USER, ASSET_DB_PASS, ASSET_DB_HOST, ASSET_DB_PORT,
    CAPA_CMS_DB_DATABASE, CAPA_CMS_DB_USER, CAPA_CMS_DB_PASS, CAPA_CMS_DB_HOST, CAPA_CMS_DB_PORT
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
