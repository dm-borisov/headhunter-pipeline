import requests
import logging
from sqlalchemy import insert
from sqlalchemy.schema import Table
from sqlalchemy.engine import Engine
from tables import get_table, engine


FORMAT = "[%(asctime)s] {%(filename)s} %(levelname)s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)


def get_page(url: str, params: dict | None = None,
             headers: dict | None = None) -> dict:
    """
    Return json of the requested page

    Parameters
    ----------
    url: str
        URL of the requested page
    headers: dict
        optional headers for the request
    params: dict
        optional request's parameters, like query, date_from, etc

    Returns
    -------
    JSON-like dict of the page's data
    """

    try:
        page = requests.get(url, params=params, headers=headers)
        page.raise_for_status()
        logging.info(f"get data from {url}")
    except requests.exceptions.HTTPError as e:  # Check for 404 and 403 errors
        logging.error(e)
        raise SystemExit()

    return page.json()


def write_to_db(data: list, table: Table, engine: Engine):
    """
    Writes data from head-hunter dictionary to database.

    Parameters
    ----------
    data: list
        A list of dictionaries with necessary data
    table: Table
        A table to write in
    engine: Engine
        An engine for database
    """
    with engine.connect() as conn:
        for row in data:
            conn.execute(insert(table), row)
            conn.commit()


if __name__ == "__main__":
    data = get_page("https://api.hh.ru/dictionaries")

    schedule = get_table("schedule")
    write_to_db(data["schedule"], schedule, engine)

    experience = get_table("experience")
    write_to_db(data["experience"], experience, engine)
