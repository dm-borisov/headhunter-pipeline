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


def get_industries(data: list) -> list:
    """
    Make a list of industries in <id, parent_id, name> format

    Parameters
    ----------
    data: list
        A list of industries

    Yields
    -------
    An industries list
    """
    industries = []
    for obj in data:
        industries.append({
            "id": obj["id"],
            "parent_id": None,
            "name": obj["name"]
        })
        for child_obj in obj["industries"]:
            industries.append({
                "id": child_obj["id"],
                "parent_id": obj["id"],
                "name": child_obj["name"]
            })

    return industries


def get_areas(data: list) -> list:
    """
    Make a list of areas in <id, parent_id, name> format

    Parameters
    ----------
        data: list
            A list of areas with parent-child relationship

    Returns
    -------
        An areas list
    """
    areas = []
    for obj in data:
        areas.append({
            "id": obj["id"],
            "parent_id": obj["parent_id"],
            "name": obj["name"]
        })
        if obj["areas"]:
            areas.extend(get_areas(obj["areas"]))

    return areas


def write_to_db(data: list, table: Table, engine: Engine):
    """
    Writes data to database.

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

    logging.info(f"writing data to {table} is completed")


if __name__ == "__main__":
    areas = get_table("areas")
    industries = get_table("industries")

    data = get_page("https://api.hh.ru/areas")

    areas_data = get_areas(data)
    write_to_db(areas_data, areas, engine)

    data = get_page("https://api.hh.ru/industries")

    industries_data = get_industries(data)
    write_to_db(industries_data, industries, engine)
