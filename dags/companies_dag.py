from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.docker_operator import DockerOperator
from airflow.operators.postgres_operator import PostgresOperator


from datetime import timedelta, datetime
from docker.types import Mount


default_args = {
    "owner": "airflow",
    "description": "ETL process for headhunter api data (companies)",
    "depend_on_past": False,
    "start_date": datetime(2023, 8, 16),
    "retries": 1,
    "retry_delay": timedelta(minutes=5)
}

with DAG("companies_dag",
         default_args=default_args,
         schedule_interval="@daily",
         catchup=False) as dag:

    with TaskGroup(group_id="create") as create_tg:
        create_companies_tmp_table = PostgresOperator(
            task_id="create_companies_tmp_table",
            postgres_conn_id="hh-data",
            sql="sql/create_companies_tmp_table.sql"
        )

        create_industries_tmp_table = PostgresOperator(
            task_id="create_industries_tmp_table",
            postgres_conn_id="hh-data",
            sql="sql/create_industries_tmp_table.sql"
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
                ' --key employer --filename companies-{{ ds }}'
            )

            tasks.append(DockerOperator(
                task_id="extract_companies_"+experience,
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
            "accredited_it_employer",
            "name",
            "type",
            "area_id",
            "site_url",
            "alternate_url"
        ))

        cmd = (
            "table companies_{{ ds_nodash }} -l " + columns +
            " --filename companies-{{ ds }}"
        )
        transform_companies = DockerOperator(
            task_id="transform_companies",
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
            "attribute industries_{{ ds_nodash }} -k industries -s id "
            "-a industry_id --filename companies-{{ ds }}"
        )
        keys = "-k industries -s id -a industry_id"
        transform_industries = DockerOperator(
            task_id="transform_industries",
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
        insert_companies = PostgresOperator(
            task_id="insert_companies",
            postgres_conn_id="hh-data",
            sql="sql/insert_companies.sql"
        )

        insert_industries = PostgresOperator(
            task_id="insert_industries",
            postgres_conn_id="hh-data",
            sql="sql/insert_industries.sql"
        )

        insert_companies >> insert_industries

    with TaskGroup(group_id="clean") as clean_tg:
        drop_companies_tmp = PostgresOperator(
            task_id="drop_companies_tmp",
            postgres_conn_id="hh-data",
            sql="sql/drop_companies_tmp.sql"
        )

        drop_industries_tmp = PostgresOperator(
            task_id="drop_industries_tmp",
            postgres_conn_id="hh-data",
            sql="sql/drop_industries_tmp.sql"
        )

        clean_volume = DockerOperator(
            task_id="clean_volume",
            image="debian:stable-slim",
            api_version="auto",
            auto_remove=True,
            command="/bin/bash -c 'rm ./data/companies-{{ ds }}.jsonl'",
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

    create_tg >> extract_tg >> transform_tg >> load_tg >> clean_tg
