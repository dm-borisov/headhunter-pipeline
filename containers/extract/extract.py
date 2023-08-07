import requests
import jsonlines
from time import sleep
from typing import Generator


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

        sleep(0.5)


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
            sleep(0.5)


if __name__ == "__main__":
    URL = 'https://api.hh.ru/vacancies'
    QUERY = 'Name:(data engineer OR machine learning OR data science)'
    WAIT = 1.0

    params = {
        'text': QUERY,
        'date_from': '2023-07-29',
        'date_to': '2023-07-29',
        'per_page': 100,
        'page': 0
    }
    
    get_data(URL, params, "file.jsonl")