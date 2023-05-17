from datetime import timedelta
from operators.ingest_data_operator import IngestDataOperator
import pendulum
from airflow.models.dag import dag

DAG_ID = "sample"
DEFAULT_ARGS = {
    "depends_on_past": False,
    "start_date": pendulum.datetime(2021, 1, 1, tz="Asia/Singapore"),
}

@dag(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    catchup=False,
    schedule_interval=timedelta(minutes = 5),
    is_paused_upon_creation=True,
    description="Just a sample DAG",
)
def create_sample_dag():
    IngestDataOperator(
        task_id="process_document"
    )


globals()[DAG_ID] = create_sample_dag()
