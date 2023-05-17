# import json
# from airflow.models.connection import Connection
import os

# c = Connection(
#      conn_id="some_conn",
#      conn_type="mssql",
#      description="connection description",
#      host="myhost.com",
#      login="myname",
#      password="mypassword",
#      extra=json.dumps(dict(this_param="some val", that_param="other val*")),
#  )


# print(f"AIRFLOW_CONN_{c.conn_id.upper()}='{c.get_uri()}'")

# IMPORT THE SQALCHEMY LIBRARY's CREATE_ENGINE METHOD
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

# DEFINE THE DATABASE CREDENTIALS
user = 'sa'
password = 'newcross_123'
host = '127.0.0.1'
port = 1433
database = 'master'

print("mssql+pyodbc://{0}:{1}@{2}:{3}/{4}".format(user, password, host, port, database))


# PYTHON FUNCTION TO CONNECT TO THE MYSQL DATABASE AND
# RETURN THE SQLACHEMY ENGINE OBJECT
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