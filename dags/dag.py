from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.docker_operator import DockerOperator

from datetime import timedelta
from docker.types import Mount


default_args = {
    "owner": "airflow",
    "description": "extract data from hh",
    "depend_on_past": False,
    "start_date": days_ago(1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG(
    "docker_dag",
    default_args=default_args,
    schedule_interval="@daily") as dag:

    extract_data = DockerOperator(
        task_id="extract_data",
        image="example",
        api_version="auto",
        auto_remove=True,
        command="python extract.py",
        docker_url="tcp://docker-proxy:2375",
        mount_tmp_dir=False,
        mounts = [
            Mount(
                source="hh-pipeline-data",
                target="/app/data",
                type="volume")
        ],
        network_mode="bridge"
    )

    transform_data = DockerOperator(
        task_id="transform_data",
        image="example2",
        api_version="auto",
        auto_remove=True,
        command="python transform.py",
        docker_url="tcp://docker-proxy:2375",
        mount_tmp_dir=False,
        mounts = [
            Mount(
                source="hh-pipeline-data",
                target="/app/data",
                type="volume")
        ],
        network_mode="bridge"       
    )


    extract_data >> transform_data
