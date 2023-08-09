import argparse
import requests
import jsonlines
from time import sleep
from typing import Generator
from random import uniform


URL: str = 'https://api.hh.ru/vacancies'
PATH: str = 'data/extracted.jsonl'
MIN_WAIT: float = 0.5  # values less than that get captcha
MAX_WAIT: float = 1.0


def get_page(url: str, headers: dict = None, params: dict = None) -> dict:
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

    page = requests.get(url, params=params)
    return page.json()


def get_urls(url: str, params: dict,
             headers: dict, key: str = None) -> Generator[str, None, None]:
    """
    Yield url from requested pages

    Parameters
    ----------
    url: str
        URL of the requested page
    params: dict
        optional parameters for the request
    headers: dict
        optional headers for the request
    key: str
        optioal key for retrieving from nested data

    Returns
    -------
    URL for the certain page
    """

    num_of_pages = int(get_page(url, params=params)['pages'])
    for page_num in range(num_of_pages):
        params['page'] = page_num

        for item in get_page(url, params)['items']:
            yield item['url'] if key is None else item[key]['url']

        sleep(uniform(MIN_WAIT, MAX_WAIT))


def get_data(url: str, params: dict, path: str, key: str = None):
    """
    Write data into jsonlike-file

    Parameters
    ----------
    url: str
        URL of the requested page
    params: dict
        optional parameters for the request
    path: str
        path to the jsonlike-file
    headers: dict
        optional headers for the request
    key: str
        optioal key for retrieving from nested data
    """

    with jsonlines.open(path, mode='w') as writer:
        for url in get_urls(url, params, key):
            writer.write(get_page(url))
            print(f'GET VACANCY FROM {url}')
            sleep(uniform(MIN_WAIT, MAX_WAIT))


if __name__ == "__main__":
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
    params = vars(parser.parse_args())

    get_data(URL, params, PATH)
