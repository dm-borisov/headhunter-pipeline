from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table, Column, String, Integer, DateTime, Boolean


metadata_obj = MetaData()

vacancies_table = Table(
    "vacancies_tmp",
    metadata_obj,
    Column("id", Integer),
    Column("name", String),
    Column("area_id", Integer),
    Column("employer_id", Integer),
    Column("published_at", DateTime),
    Column("experience_id", String),
    Column("schedule_id", String),
    Column("professional_roles", Integer),
    Column("alternate_url", String),
    Column("salary_from", Integer),
    Column("salary_to", Integer),
    Column("salary_currency", String),
    Column("salary_gross", Boolean)
)

skills_table = Table(
    "skills_tmp",
    metadata_obj,
    Column("id", Integer),
    Column("skill", String)
)

companies_table = Table(
    "companies_tmp",
    metadata_obj,
    Column("id", Integer),
    Column("accredited_it_employer", Boolean),
    Column("name", String),
    Column("type", String),
    Column("area_id", Integer),
    Column("site_url", Integer),
    Column("alternate_url", Integer)
)

industries_talbe = Table(
    "industries_tmp",
    metadata_obj,
    Column("id", Integer),
    Column("industry", String)
)

path = "postgresql+psycopg2://postgres:postgres@172.17.0.1/postgres"
engine = create_engine(path)
metadata_obj.create_all(engine)

tables = {
    "vacancies_tmp": vacancies_table,
    "skills_tmp": skills_table,
    "companies_tmp": companies_table,
    "industries_tmp": industries_talbe
}
