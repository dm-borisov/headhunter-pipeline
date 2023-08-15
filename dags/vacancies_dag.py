from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.postgres_operator import PostgresOperator


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
    "vacancies_dag",
    default_args=default_args,
    schedule_interval="@daily") as dag:

    cmd = ('"Name:(data engineer OR data analyst)" '
           '{{ ds }} {{ ds }} between3And6 --filename vacancies')
    extract_vacancies = DockerOperator(
        task_id="extract_vacancies",
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
        command=f"table vacancies_tmp -l {columns} --filename vacancies",
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
        command=f"attribute skills_tmp {keys} --filename vacancies",
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

    insert_vacancies = PostgresOperator(
        task_id="insert_vacancies",
        postgres_conn_id="hh-data",
        sql="sql/insert_vacancies.sql"
    )

    insert_skills = PostgresOperator(
        task_id="insert_skills",
        postgres_conn_id="hh-data",
        sql="sql/insert_skills.sql"
    )

    truncate_vacancies_tmp = PostgresOperator(
        task_id="truncate_vacancies_tmp",
        postgres_conn_id="hh-data",
        sql="sql/truncate_tmp_vacancies.sql"
    )

    truncate_skills_tmp = PostgresOperator(
        task_id="truncate_skills_tmp",
        postgres_conn_id="hh-data",
        sql="sql/truncate_tmp_skill.sql"
    )

    (extract_vacancies >> [transform_vacancies, transform_skills]
     >> insert_vacancies >> insert_skills >>
     [truncate_vacancies_tmp, truncate_skills_tmp])
