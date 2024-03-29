# THIRD-PARTY
import logging
import structlog
from requests_html import HTMLSession

# LOCAL-APP
from .utils import scraper_exception_handler


@scraper_exception_handler()
def fetch_page(url, header=None):
    '''Fetches HTML from URL
    :param str URL: URL to fetch.

    :return: requests.html or None
    '''
    logger = structlog.getLogger()

    if not header:
        headers = {
            'user-agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_6) AppleWebKit/537.36'
            '(KHTML, like Gecko) Chrome/53.0.2785.143 Safari/537.36'}
    else:
        headers = header

    session = HTMLSession()
    response = session.get(url, headers=headers, timeout=10)
    log = logger.bind(url=url, status_code=response.status_code)

    if response.status_code == 200:
        log.debug('Successfully Connected...')
        return response.html
    else:
        log.debug('Connection Error...')
        return None
