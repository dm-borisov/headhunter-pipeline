import argparse
from fake_useragent import UserAgent
from utils import get_data


URL = 'https://api.hh.ru/vacancies'
PATH = 'data/'


def parser_init() -> dict:
    """Initializes cli parser."""

    parser = argparse.ArgumentParser(
        prog="Extractor",
        description="Pull data from hh-api into .jsonl files",
    )
    parser.add_argument("text", help="search query")
    parser.add_argument("date_from", help="start date in YYYY-MM-DD format")
    parser.add_argument("date_to", help="end data in YYYY-MM-DD format")
    parser.add_argument("experience", help="experience search key")
    parser.add_argument("--per_page", type=int, default=100,
                        help="objects for page")
    parser.add_argument("--page", type=int, default=0, help="page number")
    parser.add_argument("--key", help="an object's key to retrieve url")
    parser.add_argument("--filename", help="name of file to store data",
                        required=True)

    return vars(parser.parse_args())


if __name__ == "__main__":

    params = parser_init()

    key = params["key"]
    full_path = PATH + params["filename"] + '.jsonl'
    # Delete key and filename fields to leave parameters for query only
    del params["key"]
    del params["filename"]

    ua = UserAgent()
    headers = {"User-Agent": ua.random}

    get_data(URL, params, headers, full_path, key)
