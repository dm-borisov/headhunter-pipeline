from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, String, Integer


metadata_obj = MetaData()

vacancies_table = Table(
    "vacancies_tmp",
    metadata_obj,
    Column("id", Integer),
    Column("name", String),
    Column('alternate_url', String),
    Column('salary_from', Integer),
    Column('salary_to', Integer),
    Column('published_at', String)
)

skills_table = Table(
    "skills_tmp",
    metadata_obj,
    Column("id", Integer),
    Column("skill", String)
)

path = "postgresql+psycopg2://postgres:postgres@172.17.0.1/postgres"
engine = create_engine(path)
metadata_obj.create_all(engine)
