from sqlalchemy import create_engine
from sqlalchemy import MetaData
# from sqlalchemy import Table
from sqlalchemy import Table, Column, Integer, String

PATH = "postgresql+psycopg2://postgres:postgres@172.17.0.1/postgres"


def get_table(table_name):
    """
    Connects to temporary table

    Parameters
    ----------
    table_name: str
        Name of temporary table
    """
    metadata_obj = MetaData()
    return Table(
        table_name,
        metadata_obj,
        autoload_with=engine,
        extend_existing=True)


engine = create_engine(PATH)
metadata_obj = MetaData()

areas = Table(
    "areas",
    metadata_obj,
    Column("id", Integer),
    Column("parent_id", Integer),
    Column("name", String)
)

industries = Table(
    "industries",
    metadata_obj,
    Column("id", String),
    Column("parent_id", String),
    Column("name", String)
)

schedule = Table(
    "schedule",
    metadata_obj,
    Column("id", String),
    Column("name", String)
)

experience = Table(
    "experience",
    metadata_obj,
    Column("id", String),
    Column("name", String)
)

metadata_obj.create_all(engine)
