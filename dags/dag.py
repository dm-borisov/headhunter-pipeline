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

    cmd = ('"Name:(data engineer OR data analyst)" '
           '2023-08-08 2023-08-08 between3And6')
    extract_data = DockerOperator(
        task_id="extract_data",
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
        "id name area_id employer_id published_at "
        "experience_id schedule_id professional_roles alternate_url "
        "salary_from salary_to salary_currency salary_gross")
    transform_vacancies = DockerOperator(
        task_id="transform_vacancies",
        image="example2",
        api_version="auto",
        auto_remove=True,
        command=f"table vacancies_tmp -l {columns}",
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

    keys = "-k key_skills -s name -a skill"
    transform_skills = DockerOperator(
        task_id="transform_skills",
        image="example2",
        api_version="auto",
        auto_remove=True,
        command=f"attribute skills_tmp {keys}",
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

    extract_data >> [transform_vacancies, transform_skills]
