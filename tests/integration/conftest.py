import logging
import os
from datetime import datetime

import pytest
import requests
import s3fs
from requests.adapters import HTTPAdapter
from urllib3 import Retry
from testcontainers.compose import DockerCompose

from tests.integration.db_connection import get_connection
from tests.integration.db_connection import setup_database, teardown_database


def assert_container_is_ready(readiness_check_url) -> requests.Session:
    request_session = requests.Session()
    retries = Retry(
        total=20,
        backoff_factor=0.2,
        status_forcelist=[404, 500, 502, 503, 504],
    )
    request_session.mount("http://", HTTPAdapter(max_retries=retries))
    assert request_session.get(readiness_check_url)
    return request_session


class FixtureDataBase(object):
    def __init__(self):
        self.engine = get_connection()
        res = self.engine.execute("SELECT * FROM sys.databases WHERE name = N'ncintegration'")
        if res:
            res = res.all()

        if not res:
            self.engine.execute("CREATE DATABASE ncintegration")
    def __enter__(self):
        setup_database(self.engine)
    def __exit__(self, sometype, value, traceback):
        teardown_database(self.engine)


class FixtureS3(object):
    def __init__(self):
        self.s3 = s3fs.S3FileSystem(
            key="AKIAIOSFODNN7EXAMPLE",
            secret="wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            client_kwargs=dict(endpoint_url="http://127.0.0.1:9444"),
        )
        self.s3.mkdir("mybucket/ingestions", acl="public-read-write", create_parents=True)

    def __enter__(self):
        files = self.s3.ls("mybucket/ingestions")
        for file in files:
            self.s3.rm(file)
        return self

    def __exit__(self, sometype, value, traceback):
        files = self.s3.ls("mybucket/ingestions")
        for file in files:
            self.s3.rm(file)



@pytest.fixture(scope="session", autouse=True)
def auto_resource(request):
    compose = DockerCompose(".", compose_file_name='docker-compose.yaml', pull=True)
    compose.start()
    compose.wait_for('http://localhost:8080')


    def auto_resource_fin():
        compose.stop()

    request.addfinalizer(auto_resource_fin)