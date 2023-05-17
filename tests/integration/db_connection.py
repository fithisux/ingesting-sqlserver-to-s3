import os

from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# DATABASE CREDENTIALS
user = 'sa'
password = 'newcross_123'
host = '127.0.0.1'
port = 1433
database = 'master'

print("mssql+pyodbc://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database))

def get_connection() -> Engine:
    return create_engine(
        url="mssql+pyodbc://{0}:{1}@{2}:{3}/{4}?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server".format(
            user, password, host, port, database
        ), connect_args = {'autocommit':True}
    )

def setup_database(engine: Engine):
    with open(f'{os.path.dirname(os.path.abspath(__file__))}/setup_database.sql') as f:
        setup_database_query: str = ''.join(f.readlines())
        engine.execute(setup_database_query)


def teardown_database(engine: Engine):
    with open(f'{os.path.dirname(os.path.abspath(__file__))}/teardown_database.sql') as f:
        teardown_database_query: str = ''.join(f.readlines())
        engine.execute(teardown_database_query)