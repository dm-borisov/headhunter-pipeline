import jsonlines
import logging
from data_fields import vacancy_keys2
from flatten_json import flatten
from sqlalchemy import insert
from sqlalchemy.schema import Table
from sqlalchemy.engine import Engine
from models import engine, skills_table, vacancies_table
from typing import Callable
from functools import wraps


PATH = "extracted.jsonl"

FORMAT = "[%(asctime)s] {%(filename)s} %(levelname)s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)


class Transformer:
    """
    A class that implements read and write functionality
    to processing functions.

    Attributes
    ----------
    path: str
        path to the file with extracted data from head-hunter
    table: Table
        table to write processed data
    engine: Engine
        Instance of Engine class to create a connection with the database

    Methods
    -------
    __call__(func)
        Adds read and write functionality to processing function
    """

    def __init__(self, path: str, table: Table, engine: Engine):
        """
        Attributes
        ----------
        path: str
            path to the file with extracted data from head-hunter
        table: Table
            table to write processed data
        engine: Engine
            Instance of Engine class to create a connection with the database
        """
        self.__path = path
        self.__table = table
        self.__engine = engine

    def __call__(self, func: Callable):
        """
        Adds read and write functionality to processing function

        Parameters
        ----------
        func: Callable
            processing function
        """
        @wraps(func)
        def wrapper(*args, **kwargs):
            with (jsonlines.open(self.__path, "r") as reader,
                  self.__engine.connect() as conn):
                for obj in reader:
                    data: dict = func(obj, *args, **kwargs)
                    if data:
                        conn.execute(insert(self.__table), data)
                        # conn.commit()

                logging.info(f"writing data to {self.__table} is completed")

        return wrapper


@Transformer(PATH, vacancies_table, engine)
def process_fields(obj: dict, keys: list) -> dict:
    """
    Retrieve necessary fields from object

    Parameters
    ----------
    obj: dict
        An item's dictionary retrieved from head-hunter api
    keys: dict
        A list of fields to retrieve

    Returns
    -------
    A dictionary with necessary fields
    """

    # Flatten nested dictionary to easily retrieve fields
    flatten_obj = flatten(obj)

    items = dict.fromkeys(keys)
    for key in keys:
        if key in flatten_obj.keys():
            items[key] = flatten_obj[key]

    return items


@Transformer(PATH, skills_table, engine)
def process_skills(obj: dict):
    return [{'id': obj['id'], 'skill': skill['name']}
            for skill in obj['key_skills']]


if __name__ == "__main__":

    process_fields(vacancy_keys2)
    process_skills()
