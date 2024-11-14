import pymssql
import logging


class DatabaseClient:
    def __init__(self, database, username, password, host):
        self.database = database
        self.username = username
        self.password = password
        self.host = host
        self.logger = logging.getLogger(__name__)

        self.connection = None
        self.cursor = None

    def get_connection(self):
        try:
            if not self.connection:
                self.connection = pymssql.connect(host=self.host, user=self.username, password=self.password, database=self.database)
            return self.connection
        except Exception as e:
            self.logger.error(f"Error connecting to database: {e}")

    def get_cursor(self):
        try:
            if not self.cursor:
                self.cursor = self.get_connection().cursor()
            return self.cursor
        except Exception as e:
            self.logger.error(f"Error getting cursor: {e}")

    def execute_sql(self, sql):
        try:
            cur = self.get_cursor()
            cur.execute(sql)
            if cur.description:
                return cur.fetchall()
            else:
                self.connection.commit()
                return None
        except Exception as e:
            self.logger.error(f"Error executing SQL: {e}")
            return None
