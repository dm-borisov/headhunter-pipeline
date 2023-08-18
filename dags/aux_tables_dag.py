from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta


default_args = {
    "owner": "airflow",
    "description": "Creates auxiliary tables",
    "depend_on_past": False,
    "start_date": datetime(2023, 8, 16),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("aux_tables_dag",
         default_args=default_args,
         schedule_interval="@once") as dag:

    load_aux_tables = DockerOperator(
        task_id="load_aux_tables",
        image="dmborisov/headhunter-pipeline:auxiliary-extract",
        api_version="auto",
        auto_remove=True,
        command="",
        docker_url="tcp://docker-proxy:2375",
        mount_tmp_dir=False,
        network_mode="bridge"
    )

    load_aux_tables
