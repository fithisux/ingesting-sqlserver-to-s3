# Airflow integration testing


## Background

This code is for tutorial purposes of the Agile Actors Data Chapter. It continues in a more advanced manner from
[Basic integration Testing](https://github.com/fithisux/airflow-integration-testing) in the spirirt of
[Testing in Airflow Part 2 — Integration Tests and End-To-End Pipeline Tests](https://medium.com/@chandukavar/testing-in-airflow-part-2-integration-tests-and-end-to-end-pipeline-tests-af0555cd1a82)

In summary the project sets up an SQL Server with "transactions". Airflow scans the table and when it finds new transactions, 
it ingests the transactions in S3Ninja emulating S3 store. Then it marks in another table that the task has been accomplished.

## Integration testing

Tested with python 3.11.3.

First install dependencies

```bash
pip install -r requirements-dev.txt
```

and then build "good" image.

```bash
docker compose build
```

Now testing is as easy as executing

```bash
 rm -rf logs s3data s3logs
 pytest -vvv -s --log-cli-level=DEBUG tests/integration/test_automatically_sample_dag.py
```

The integration tests (not complete) create the bucket in [docker-compose.yaml](docker-compose.yaml), 
the test database, user, schema and table also in [docker-compose.yaml](docker-compose.yaml). Then it runs tha dag.

## Manual testing

Here we run manually the steps leading to the integration tests. 
For maintaining state uncomment the sections in docker compose for volumes, not recommended. 
Please comment them again before running the integration tests.

Start the deployment by running

```bash
rm -rf logs s3data s3logs
docker compose up
```

Now you need to what is in [docker-compose.yaml](docker-compose.yaml).

### Create bucket

Connect to [S3Ninja](http://127.0.0.1:9444/ui) and create a bucket named `mybucket`. This is the name that appears in airflow variable.


### Create users and tables.

Connect with DBeaver to the SQL Server with credentials

```python
user = 'sa'
password = 'newcross_123'
host = '127.0.0.1'
port = 1433
database = 'master'
```

Now for cleaning everything up (if you need it),  just run [teardown_database.sql](tests/integration/teardown_database.sql)

To create the database used in the deployment execute

```sql
IF NOT EXISTS (SELECT * FROM sys.databases WHERE name = N'ncintegration')
    BEGIN
        CREATE DATABASE ncintegration;
    END
```

and to create the rest, just run [setup_database.sql](tests/integration/setup_database.sql)

You can optionally for your tests add

```sql
delete from ncproject.fintransacts;
insert into  ncproject.fintransacts values (1, convert(DATETIME, '1968-10-23 12:45:37', 20), 'buy 1');
insert into  ncproject.fintransacts values (2, convert(DATETIME, '1968-10-24 12:45:37', 20), 'buy 2');
insert into  ncproject.fintransacts values (3, convert(DATETIME, '1968-10-25 12:45:37', 20), 'buy 3');
insert into  ncproject.fintransacts values (4, convert(DATETIME, '1968-10-26 12:45:37', 20), 'buy 4');
insert into  ncproject.fintransacts values (5, convert(DATETIME, '1968-10-27 12:45:37', 20), 'buy 5');
```

### Run the dag

Visit the [airflow server](http://localhost:8080) and activate your dag. Extra info in official Airflow Documentation.
[Airflow Apache Project](https://airflow.apache.org/).

Have fun!