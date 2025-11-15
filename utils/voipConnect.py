# context manager
import mysql.connector

class VoipDBConnection:
    def __init__(self, host, user, password, database):
        self.conn = mysql.connector.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.conn.cursor()

    def __enter__(self):
        return self.conn

    def __exit__(self, _exc_type, _exc_value, _traceback):
        self.cursor.close()