from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from tests.integration.conftest import FixtureDataBase, FixtureS3
from tests.integration.dag_trigger import run_dag

SAMPLE_DAG_ID = "sample"

def test_ingestion_happens_succesfully():

   with FixtureDataBase():
       with FixtureS3() as s3_fixture:
           ingestion_happens_succesfully(s3_fixture)


def ingestion_happens_succesfully(s3_fixture: FixtureS3):
    engine: Engine = create_engine(
        url='mssql+pyodbc://nclogin:ncuser123!!@127.0.0.1:1433/ncintegration?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server', connect_args={'autocommit': True}
    )

    engine.execute("""
    insert into  ncproject.fintransacts values (1, convert(DATETIME, '1968-10-23 12:45:37', 20), 'buy 1');
    """)

    run_dag(SAMPLE_DAG_ID)

    res = engine.execute("""
            select * from ncproject.ingestions ;
            """)

    ingestion_date = res.all()[0][2]

    # we are testing that way because we may have spurious double runs
    assert len(list(s3_fixture.s3.ls(f"mybucket/ingestions/{ingestion_date}"))) == 1

    engine.execute("""
        delete from ncproject.ingestions ;
        """)

    engine.execute("""
        delete from ncproject.fintransacts ;
        """)


def test_no_need_to_ingest():

   with FixtureDataBase():
       with FixtureS3() as s3_fixture:
           no_need_to_ingest(s3_fixture)

def no_need_to_ingest(s3_fixture: FixtureS3):
    engine: Engine = create_engine(
        url='mssql+pyodbc://nclogin:ncuser123!!@127.0.0.1:1433/ncintegration?TrustServerCertificate=yes&driver=ODBC+Driver+18+for+SQL+Server', connect_args={'autocommit': True}
    )

    engine.execute("""
        insert into ncproject.ingestions values (1, convert(DATETIME, '1968-10-23 12:45:37', 20), '1682591431.352781');
        """)

    engine.execute("""
    insert into ncproject.fintransacts values (1, convert(DATETIME, '1968-10-23 12:45:37', 20), 'buy 1');
    """)

    run_dag(SAMPLE_DAG_ID)

    assert len(list(s3_fixture.s3.ls("mybucket/ingestions/{ingestion_date}"))) == 0

    engine.execute("""
            delete from ncproject.ingestions ;
            """)

    engine.execute("""
            delete from ncproject.fintransacts ;
            """)
