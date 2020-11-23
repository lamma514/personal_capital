# Python Standard Library
import os

# pypi
from dotenv import load_dotenv
import psycopg2

# Local Modules

class PostgresConn:
    def __init__(self):

        load_dotenv('.env')

        HOSTNAME = os.getenv("POSTGRES_PERSONAL_CAPITAL_HOSTNAME")
        PORT     = os.getenv("POSTGRES_PERSONAL_CAPITAL_PORT")
        DATABASE = os.getenv("POSTGRES_PERSONAL_CAPITAL_DATABASE")
        USER     = os.getenv("POSTGRES_PERSONAL_CAPITAL_USER")
        PASSWORD = os.getenv("POSTGRES_PERSONAL_CAPITAL_PASSWORD")

        connection_string = f"host='{HOSTNAME}' port='{PORT}' dbname='{DATABASE}' user='{USER}' password='{PASSWORD}'"

        self.conn = psycopg2.connect(connection_string)
        self.conn.set_session(autocommit=True)

    def __del__(self):
        
        print(f"{__name__}: Closing connection.")
        self.conn.close()

    def execute(self, sql):

        with self.conn.cursor() as cursor:
            cursor.execute(sql)

    def query(self, sql):
        
        with self.conn.cursor() as cursor:
            cursor.execute(sql)
            rows = cursor.fetchall()
            for row in rows:
                yield row

    def copy_from(self, table_name, path, sep=',', null=''):

        with open(path, 'r') as fd:
            with self.conn.cursor() as cursor:
                cursor.copy_from(fd, table_name, sep=sep, null=null)


if __name__ == '__main__':

    p = PostgresConn()
    p = None

