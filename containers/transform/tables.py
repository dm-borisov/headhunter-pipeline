from sqlalchemy import create_engine
from sqlalchemy import MetaData
from sqlalchemy import Table
from environs import Env


# Load environment data
env = Env()
env.read_env()

user = env("POSTGRES_USER")
password = env("POSTGRES_PASSWORD")
db = env("POSTGRES_DB")
url = env("CONN_URL")

PATH = f"postgresql+psycopg2://{user}:{password}@{url}/{db}"


def get_table(table_name) -> Table:
    """
    Returns a specified table

    Parameters
    ----------
    table_name: str
        Name of a table

    Returns
    -------
        A specified table
    """
    metadata_obj = MetaData()
    return Table(
        table_name,
        metadata_obj,
        autoload_with=engine,
        extend_existing=True)


engine = create_engine(PATH)
