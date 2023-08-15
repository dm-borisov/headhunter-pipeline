import argparse
from exceptions import FlagError
from processors import TableProcessor, AttributeProcessor
from tables import engine, get_table


PATH = "data/"


def parser_init() -> dict:
    """Initializes cli parser."""

    parser = argparse.ArgumentParser(
        prog="Transformer",
        description="Transforms extracted data and write it to db",
    )
    parser.add_argument("method", choices=["table", "attribute"],
                        help="Choose write full table or id-attribute table")
    parser.add_argument("table_name",
                        help="Name of the database table")
    parser.add_argument("-l", "--list", action="extend", nargs="+",
                        help="A list of keys for full table")
    parser.add_argument("-k", "--key",
                        help="A nested data key for id-attribute table")
    parser.add_argument("-s", "--skey",
                        help="A sub-key for id-attribute table")
    parser.add_argument("-a", "--attribute",
                        help="An attribute name of id-attribute table")
    parser.add_argument("-f", "--filename", required=True,
                        help="name of file where data is stored")

    return vars(parser.parse_args())


def validate_keys_list(params: dict):
    """
    Raises error if there is no list or list is empty.

    Parameters
    ----------
    params: dict
        Parameters from cli
    """
    if params["list"] is None:
        raise FlagError("The list of keys is not provided.")
    if not params["list"]:
        raise FlagError("The list of keys is empty.")


def validate_attribute_keys(params: dict):
    """
    Raises error if one of the keys is missing

    Parameters
    ----------
    params: dict
        Parameters from cli
    """
    for flag in ("key", "skey", "attribute"):
        if params[flag] is None:
            raise FlagError(f"Flag {flag} is not provided.")


if __name__ == "__main__":
    params = parser_init()

    table = get_table(params["table_name"])
    if params["method"] == "table":
        validate_keys_list(params)

        processor = TableProcessor(
            params["list"],
            PATH+params["filename"]+".jsonl",
            table,
            engine)
    elif params["method"] == "attribute":
        validate_attribute_keys(params)

        processor = AttributeProcessor(
            params["key"],
            params["skey"],
            params["attribute"],
            PATH+params["filename"]+".jsonl",
            table,
            engine)

    processor.write_data()
