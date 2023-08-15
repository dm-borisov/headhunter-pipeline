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

    create_vacancies_tmp_table = PostgresOperator(
        task_id="create_vacancies_tmp_table",
        postgres_conn_id="hh-data",
        sql="sql/create_vacancies_tmp_table.sql"
    )

    create_skills_tmp_table = PostgresOperator(
        task_id="create_skills_tmp_table",
        postgres_conn_id="hh-data",
        sql="sql/create_skills_tmp_table.sql"
    )

    cmd = ('"Name:(data engineer OR data analyst)" '
           '{{ ds }} {{ ds }} between3And6 --filename vacancies-{{ ds }}')
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
        command="table vacancies_{{ ds_nodash }} "+f"-l {columns} "+"--filename vacancies-{{ ds }}",
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
        command="attribute skills_{{ ds_nodash }} "+f"{keys} "+"--filename " +"vacancies-{{ ds }}",
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

    drop_vacancies_tmp = PostgresOperator(
        task_id="drop_vacancies_tmp",
        postgres_conn_id="hh-data",
        sql="sql/drop_tmp_vacancies.sql"
    )

    drop_skills_tmp = PostgresOperator(
        task_id="drop_skills_tmp",
        postgres_conn_id="hh-data",
        sql="sql/drop_tmp_skill.sql"
    )

    ([create_skills_tmp_table, create_vacancies_tmp_table] >>
     extract_vacancies >> [transform_vacancies, transform_skills]
     >> insert_vacancies >> insert_skills >>
     [drop_vacancies_tmp, drop_skills_tmp])

    create_skills_tmp_table
    create_vacancies_tmp_table
