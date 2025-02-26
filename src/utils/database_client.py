import logging
from sqlalchemy import create_engine, text


class DatabaseClient:
    def __init__(self, database, username, password, host, db_type='mssql'):
        self.database = database
        self.username = username
        self.password = password
        self.host = host
        self.db_type = db_type.lower()
        self.logger = logging.getLogger(__name__)

        self.connection = None
        self.cursor = None

    def get_connection(self):
        try:
            if not self.connection:
                self.logger.info(f"Attempting to connect to {self.db_type} database: {self.database} at {self.host}")
                if self.db_type == 'mssql':
                    self.connection = create_engine(f'mssql+pymssql://{self.username}:{self.password}@{self.host}/{self.database}')
                elif self.db_type == 'mysql':
                    self.connection = create_engine(f'mysql+pymysql://{self.username}:{self.password}@{self.host}/{self.database}')
                else:
                    raise ValueError(f"Unsupported database type: {self.db_type}")
                self.logger.info(f"Connected to {self.db_type} database: {self.database} at {self.host}")
            return self.connection
        except Exception as e:
            self.logger.error(f"Error connecting to database: {e}")
            return None

    def get_cursor(self):
        try:
            if not self.cursor:
                self.cursor = self.get_connection().raw_connection().cursor()
            return self.cursor
        except Exception as e:
            self.logger.error(f"Error getting cursor: {e}")
            return None

    def execute_sql(self, sql, params=None):
        try:
            cur = self.get_cursor()
            if cur is None:
                raise Exception("Cursor is None, cannot execute SQL")
            if params:
                cur.execute(sql, params)
            else:
                cur.execute(sql)
            if cur.description:
                return cur.fetchall()
            else:
                self.connection.raw_connection().commit()
                return None
        except Exception as e:
            self.logger.error(f"Error executing SQL: {e}")
            return None

    def ensure_database_exists(self):
        try:
            if self.db_type == 'mssql':
                engine = create_engine(f'mssql+pymssql://{self.username}:{self.password}@{self.host}')
            elif self.db_type == 'mysql':
                engine = create_engine(f'mysql+pymysql://{self.username}:{self.password}@{self.host}')
            else:
                raise ValueError(f"Unsupported database type: {self.db_type}")

            with engine.connect() as conn:
                if self.db_type == 'mssql':
                    conn.execute(text(f"IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = '{self.database}') CREATE DATABASE {self.database}"))
                elif self.db_type == 'mysql':
                    conn.execute(text(f"CREATE DATABASE IF NOT EXISTS {self.database}"))
                self.logger.info(f"Database {self.database} ensured to exist")
        except Exception as e:
            self.logger.error(f"Error ensuring database exists: {e}")
