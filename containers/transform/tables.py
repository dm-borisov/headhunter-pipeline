from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table


PATH = "postgresql+psycopg2://postgres:postgres@172.17.0.1/postgres"


def get_table(table_name):
    """
    Connects to temporary table

    Parameters
    ----------
    table_name: str
        Name of temporary table
    prefix: str
        Prefix of temporary table
    """
    metadata_obj = MetaData()
    return Table(
        table_name,
        metadata_obj,
        autoload_with=engine,
        extend_existing=True)


engine = create_engine(PATH)
