from utils.database_client import DatabaseClient
from utils.config import SENSUM_DB_DATABASE, SENSUM_DB_USER, SENSUM_DB_PASS, SENSUM_DB_HOST, SENSUM_DB_PORT, POSTGRES_DB_USER, POSTGRES_DB_PASS, POSTGRES_DB_HOST, POSTGRES_DB_DATABASE, POSTGRES_DB_PORT


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
