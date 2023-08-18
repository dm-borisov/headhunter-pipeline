import jsonlines
import logging
from sqlalchemy import insert
from sqlalchemy.schema import Table
from sqlalchemy.engine import Engine
from typing import Generator


FORMAT = "{%(filename)s} %(levelname)s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)


class JsonlReaderMixin:
    """
    A mixin class that adds jsonl reading functionality.

    Attributes
    ------
    path: str
        A path to jsonl-file

    Methods
    -------
    read_data()
        A generator that yields one line pet time from jsonl-file
    """
    def __init__(self, path: str):
        """
        Attributes
        ----------
        path: str
            A path to jsonl-file
        """
        self.__path = path

    def read_data(self) -> Generator[dict, None, None]:
        """A generator that yields one line per time from jsonl-file"""
        with jsonlines.open(self.__path, "r") as reader:
            yield from reader


class DBWriterMixin:
    """
    A mixin class that adds writing to database functionality.

    Attributes
    ----------
    table: Table
        A database table to write processed data
    engine: Engine
        An instance of Engine class to create a connection with the database

    Methods
    -------
    write_data()
        Insert data to database from process data generator
    """
    def __init__(self, table: Table, engine: Engine):
        self.__table = table
        self.__engine = engine

    def write_data(self):
        """Insert data to database from process data generator"""
        with self.__engine.connect() as conn:
            for item in self.process_data():
                conn.execute(insert(self.__table), item)
                conn.commit()

        logging.info(f"writing data to {self.__table} is completed")
