from utils.database_client import DatabaseClient
from utils.config import (
    FRONTDESK_POSTGRES_DB_USER, FRONTDESK_POSTGRES_DB_PASS, FRONTDESK_POSTGRES_DB_HOST, FRONTDESK_POSTGRES_DB_DATABASE, FRONTDESK_POSTGRES_DB_PORT,
    BYGGESAGER_POSTGRES_DB_DATABASE, BYGGESAGER_POSTGRES_DB_USER, BYGGESAGER_POSTGRES_DB_PASS, BYGGESAGER_POSTGRES_DB_HOST, BYGGESAGER_POSTGRES_DB_PORT,
    CAPA_CMS_DB_DATABASE, CAPA_CMS_DB_USER, CAPA_CMS_DB_PASS, CAPA_CMS_DB_HOST, CAPA_CMS_DB_PORT, ASSET_DB_DATABASE, ASSET_DB_USER, ASSET_DB_PASS, ASSET_DB_HOST,
    ASSET_DB_PORT
)


def get_db_frontdesk():
    return DatabaseClient(
        db_type='postgresql',
        database=FRONTDESK_POSTGRES_DB_DATABASE,
        username=FRONTDESK_POSTGRES_DB_USER,
        password=FRONTDESK_POSTGRES_DB_PASS,
        host=FRONTDESK_POSTGRES_DB_HOST,
        port=FRONTDESK_POSTGRES_DB_PORT
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
