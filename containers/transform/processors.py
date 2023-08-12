from mixins import JsonlReaderMixin, DBWriterMixin
from flatten_json import flatten


class TableProcessor(JsonlReaderMixin, DBWriterMixin):
    """
    A class that processes extracted data as a table

    Attributes
    ----------
    keys: list
        A list of keys that represents attributes of database table
    path: str
        A path to jsonl-file
    table: Table
        A database table to write processed data
    engine: Engine
        An instance of Engine class to create a connection with the database

    Methods
    -------
    process_data()
        A generator that creates a dictionary with passed keys and populates it
        with data from extracted object
    """
    def __init__(self, keys, path, table, engine):
        """
        Attributes
        ----------
        keys: list
            A list of keys that represents attributes of a database table
        path: str
            Path to jsonl-file
        table: Table
            A database table to write processed data
        engine: Engine
            An instance of Engine class to create a connection with
            the database
        """
        self.__keys = keys
        super().__init__(path)
        super(JsonlReaderMixin, self).__init__(table, engine)

    def process_data(self):
        """
        A generator that creates a dictionary with passed keys and populates it
        with data from extracted object

        Yields
        ------
        A dictionary that represents database row
        """
        for obj in self.read_data():
            # Flatten nested dictionary to easily retrieve fields
            flatten_obj = flatten(obj)

            items = dict.fromkeys(self.__keys)
            for key in self.__keys:
                if key in flatten_obj.keys():
                    items[key] = flatten_obj[key]

            yield items


class AttributeProcessor(JsonlReaderMixin, DBWriterMixin):
    """
    A class that takes nested data and ids from extracted file,
    and yields a dictionary with them as a database row

    Attributes
    ----------
    key: str
        A key for nested data
    sub_key: str
        A key for for a certain value of nested data item
    attribute_name: str
        A name of a database's column
    path: str
        Path to jsonl-file
    table: Table
        Database table to write processed data
    engine: Engine
        Instance of Engine class to create a connection with the database

    Methods
    -------
    process_data()
        A generator that creates a pair of <id:attribute> and yields it
        to the database
    """
    def __init__(self, key, sub_key, attribute_name, path, table, engine):
        """
        Attributes
        ----------
        key: str
            A key for nested data
        sub_key: str
            A key for for a certain value of nested data item
        attribute_name: str
            A name of a database's column
        path: str
            Path to jsonl-file
        table: Table
            Database table to write processed data
        engine: Engine
            Instance of Engine class to create a connection with the database
        """
        self.__key = key
        self.__sub_key = sub_key
        self.__attribute_name = attribute_name
        super().__init__(path)
        super(JsonlReaderMixin, self).__init__(table, engine)

    def process_data(self):
        """
        A generator that creates a pair of <id:attribute> and yields it
        to the database
        """
        for obj in self.read_data():
            if obj[self.__key]:
                for item in obj[self.__key]:
                    yield {"id": obj["id"],
                           self.__attribute_name: item[self.__sub_key]}
