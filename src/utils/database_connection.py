from utils.database_client import DatabaseClient
from utils.config import (
    FRONTDESK_POSTGRES_DB_USER, FRONTDESK_POSTGRES_DB_PASS, FRONTDESK_POSTGRES_DB_HOST, FRONTDESK_POSTGRES_DB_DATABASE, FRONTDESK_POSTGRES_DB_PORT,
    BYGGESAGER_POSTGRES_DB_DATABASE, BYGGESAGER_POSTGRES_DB_USER, BYGGESAGER_POSTGRES_DB_PASS, BYGGESAGER_POSTGRES_DB_HOST, BYGGESAGER_POSTGRES_DB_PORT
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
