import argparse
import requests
import jsonlines
import logging
from time import sleep
from typing import Generator
from random import uniform
from fake_useragent import UserAgent


URL = 'https://api.hh.ru/vacancies'
PATH = 'data/extracted.jsonl'
MIN_WAIT = 0.5  # values less than that get captcha
MAX_WAIT = 1.0

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
        sleep(uniform(MIN_WAIT, MAX_WAIT))
    except requests.exceptions.HTTPError as e:  # Check for 404 and 403 errors
        logging.error(e)
        raise SystemExit()

    return page.json()


def get_urls(url: str, params: dict, headers: dict,
             key: str | None = None) -> Generator[str, None, None]:
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

    num_of_pages = int(get_page(url, params, headers)['pages'])
    for page_num in range(num_of_pages):
        params['page'] = page_num

        for item in get_page(url, params, headers)['items']:
            yield item['url'] if key is None else item[key]['url']


def get_data(url: str, params: dict, headers: dict,
             path: str, key: str | None = None):
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
        for url in get_urls(url, params, headers, key):
            writer.write(get_page(url, headers=headers))


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
    parser.add_argument("--key", help="an object's key to retrieve url")

    params = vars(parser.parse_args())
    key = params["key"]
    del params["key"]

    ua = UserAgent()
    headers = {"User-Agent": ua.random}

    get_data(URL, params, headers, PATH, key)
