from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor

from datetime import datetime, timedelta
from docker.types import Mount


default_args = {
    "owner": "airflow",
    "description": "ETL process for headhunter api data (vacancies)",
    "depend_on_past": False,
    "start_date": datetime(2023, 8, 16),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("vacancies_dag",
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False) as dag:

    wait_for_companies_dag = ExternalTaskSensor(
        task_id="wait_for_companies_dag",
        external_dag_id="companies_dag",
        poke_interval=60*2,
        timeout=60*20,
    )

    with TaskGroup(group_id="create") as create_tg:
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

    with TaskGroup(group_id="extract") as extract_tg:
        experience_tags = (
            "noExperience",
            "between1And3",
            "between3And6",
            "moreThan6"
        )

        key_words = (
            "data analyst",
            "data engineer",
            "data scientist",
            "machine learning"
        )

        tasks = []
        for experience in experience_tags:
            cmd = (
                f'"Name:({" OR ".join(key_words)})" ' +
                '{{ ds }} {{ ds }} ' + experience +
                ' --filename vacancies-{{ ds }}'
            )
            tasks.append(DockerOperator(
                task_id="extract_vacancies_"+experience,
                image="dmborisov/headhunter-pipeline:extract",
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
                network_mode="bridge",
                pool="sequential"
            ))

    with TaskGroup(group_id="transform") as transform_tg:
        columns = " ".join((
            "id",
            "name",
            "area_id",
            "employer_id",
            "published_at",
            "experience_id",
            "schedule_id",
            "professional_roles_0_name",
            "alternate_url",
            "salary_from",
            "salary_to",
            "salary_currency",
            "salary_gross"
        ))
        cmd = (
            "table vacancies_{{ ds_nodash }} -l " + columns +
            " --filename vacancies-{{ ds }}"
        )
        transform_vacancies = DockerOperator(
            task_id="transform_vacancies",
            image="dmborisov/headhunter-pipeline:transform",
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

        cmd = (
            "attribute skills_{{ ds_nodash }} -k key_skills -s name "
            "-a skill --filename vacancies-{{ ds }}"
        )
        transform_skills = DockerOperator(
            task_id="transform_skills",
            image="dmborisov/headhunter-pipeline:transform",
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

    with TaskGroup(group_id="load") as load_tg:
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

        insert_vacancies >> insert_skills

    with TaskGroup(group_id="clean") as clean_tg:
        drop_vacancies_tmp = PostgresOperator(
            task_id="drop_vacancies_tmp",
            postgres_conn_id="hh-data",
            sql="sql/drop_vacancies_tmp.sql"
        )

        drop_skills_tmp = PostgresOperator(
            task_id="drop_skills_tmp",
            postgres_conn_id="hh-data",
            sql="sql/drop_skills_tmp.sql"
        )

        clean_volume = DockerOperator(
            task_id="clean_volume",
            image="debian:stable-slim",
            api_version="auto",
            auto_remove=True,
            command="/bin/bash -c 'rm ./data/vacancies-{{ ds }}.jsonl'",
            docker_url="tcp://docker-proxy:2375",
            mount_tmp_dir=False,
            mounts=[
                Mount(
                    source="hh-pipeline-data",
                    target="/data",
                    type="volume")
            ],
            network_mode="bridge"
        )

    (wait_for_companies_dag >> create_tg >>
     extract_tg >> transform_tg >> load_tg >> clean_tg)
