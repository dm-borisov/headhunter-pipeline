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
    "companies_dag",
    default_args=default_args,
    schedule_interval="@daily") as dag:

    cmd = ('"Name:(data engineer OR data analyst)" '
           '2023-08-08 2023-08-08 between3And6 '
           '--key employer --filename companies')
    extract_companies = DockerOperator(
        task_id="extract_companies",
        image="example",
        api_version="auto",
        auto_remove=True,
        command=cmd,
        docker_url="tcp://docker-proxy:2375",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source="hh-pipeline-data",
                target="/app/data",
                type="volume")
        ],
        network_mode="bridge"
    )

    columns = (
        "id accredited_it_employer name type area_id "
        "site_url alternate_url")
    transform_companies = DockerOperator(
        task_id="transform_companies",
        image="example2",
        api_version="auto",
        auto_remove=True,
        command=f"table companies_tmp -l {columns} --filename companies",
        docker_url="tcp://docker-proxy:2375",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source="hh-pipeline-data",
                target="/app/data",
                type="volume")
        ],
        network_mode="bridge"
    )

    keys = "-k industries -s id -a industry_id"
    transform_industries = DockerOperator(
        task_id="transform_industries",
        image="example2",
        api_version="auto",
        auto_remove=True,
        command=f"attribute industries_tmp {keys} --filename companies",
        docker_url="tcp://docker-proxy:2375",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source="hh-pipeline-data",
                target="/app/data",
                type="volume")
        ],
        network_mode="bridge"
    )

    extract_companies >> [transform_companies, transform_industries]
