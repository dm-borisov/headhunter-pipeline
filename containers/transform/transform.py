import jsonlines
from data_fields import vacancy_keys2
from flatten_json import flatten
from sqlalchemy import insert, select
from sqlalchemy.schema import Table
from sqlalchemy.engine import Engine
from models import engine, skills_table, vacancies_table
from typing import Callable
from functools import wraps


PATH = "extracted.jsonl"


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
                        conn.commit()

        return wrapper


@Transformer(PATH, vacancies_table, engine)
def process_vacancies(obj: dict, keys: list) -> dict:
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

    vacancy = {}
    for key in keys:
        if key in flatten_obj.keys():
            vacancy[key] = flatten_obj[key]
        else:
            vacancy[key] = None

    return vacancy


@Transformer(PATH, skills_table, engine)
def process_skills(obj: dict):
    return [{'id': obj['id'], 'skill': skill['name']}
            for skill in obj['key_skills']]


if __name__ == "__main__":

    process_vacancies(vacancy_keys2)

    with engine.connect() as conn:
        stmt = select(skills_table)
        for row in conn.execute(stmt):
            print(row)

    process_skills()

    with engine.connect() as conn:
        stmt = select(vacancies_table)
        for row in conn.execute(stmt):
            print(row)
