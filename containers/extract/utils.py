import requests
import jsonlines
import logging
from time import sleep
from typing import Generator
from random import uniform


MIN_WAIT = 0.5  # values less than that get captcha
MAX_WAIT = 1.0

FORMAT = "{%(filename)s} %(levelname)s %(message)s"
logging.basicConfig(format=FORMAT, level=logging.INFO)


def get_page(url: str, params: dict | None = None,
             headers: dict | None = None) -> dict:
    """
    Returns json of the requested page

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
    Yields url from requested pages

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

    Yields
    -------
    URL of the certain page
    """

    num_of_pages = int(get_page(url, params, headers)['pages'])
    for page_num in range(num_of_pages):
        params['page'] = page_num

        for item in get_page(url, params, headers)['items']:
            yield item['url'] if key is None else item[key]['url']


def get_data(url: str, params: dict, headers: dict,
             path: str, key: str | None = None):
    """
    Writes data into jsonlike-file

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

    with jsonlines.open(path, mode='a') as writer:
        for url in get_urls(url, params, headers, key):
            writer.write(get_page(url, headers=headers))
