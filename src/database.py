import sqlalchemy
import logging

import config.config


logger = logging.getLogger(__name__)


def get_database_connection(database):
    database = database.upper()
    if database == 'CLIMATE':
        port = getattr(config.config, database + '_DB_PORT')
        databasetype = 'mariadb'
    elif database == 'FRONTDESK':
        port = None
        databasetype = 'mssql'
    else:
        logger.error(f"Invalid database {database}")
        return None

    if databasetype == 'mssql':
        driver = 'mssql+pymssql'
    elif databasetype == 'mariadb':
        driver = 'mariadb+mariadbconnector'
    elif databasetype == 'postgresql':
        driver = 'postgresql+psycopg2'
    else:
        logger.error(f"Invalid database type {databasetype}")
        return None

    database = database.upper()

    host = getattr(config.config, database + '_DB_HOST')
    username = getattr(config.config, database + '_DB_USER')
    password = getattr(config.config, database + '_DB_PASS')
    db = getattr(config.config, database + '_DB_DATABASE')

    if port:
        host = host + ':' + port

    engine = sqlalchemy.create_engine(f'{driver}://{username}:{password}@{host}/{db}')

    return engine.connect()
