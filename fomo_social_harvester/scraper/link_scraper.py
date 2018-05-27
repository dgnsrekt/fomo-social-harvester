# STANDARDLIB
from collections import defaultdict
from time import sleep, time
from urllib.parse import urlparse
import json

# THIRD-PARTY
from requests_html import HTMLSession
from tenacity import *
import logging
import structlog
import tqdm

# LOCAL-APP
from .base import fetch_page
from .utils import is_valid_telegram_link, is_valid_twitter_link
from .utils import scraper_exception_handler


def parse_maxpages():
    '''Fetches number of pages from coinmarketcap.
    :return: Number of pages to parse.
    :rtype: int'''

    url = 'https://api.coinmarketcap.com/v2/global/'
    data = fetch_page(url)
    maxpages = int(json.loads(data.html)['data']['active_cryptocurrencies']) // 100
    return maxpages


def get_coinmarketcap_links(limit=None):
    '''Parses coinmarketcap mainpages for #markets links.

    :param int limit: Limits the amount of links returned.

    :return: links parsed from coinmarketcap.
    :rtype: list('/currencies/{}/#markets')'''

    logger = structlog.getLogger()

    url = 'https://coinmarketcap.com/'

    links = []
    page_data = fetch_page(url)

    if limit == None:
        total_pages = parse_maxpages()
    else:
        total_pages = (limit // 100) + 1

    with tqdm.tqdm(total=total_pages) as pbar:
        for html in page_data:
            # log = logger.bind(url=html.url)
            # log.debug('Parsing.')

            for link in html.absolute_links:
                if '#markets' in link:
                    links.append(link)
            # log.debug(f'Found {len(links)} links.')

            if limit:
                if len(links) > limit:
                    pbar.update(1)
                    return links[:limit]
            pbar.update(1)
    return links


def get_elements_and_links_from_url(url):
    '''Takes coinmarketcap #markets link

    :param str url:

    :return: unstyled list element
    :rtype: requests_html.Element'''

    html = fetch_page(url)

    selector = 'body > div.container > div > div.col-lg-10 > ' \
        'div.row.bottom-margin-2x > div.col-sm-4.col-sm-pull-8'
    if html:
        element = html.find(selector)[0]
        links = html.absolute_links
        return element, links
    else:
        return None, None


def get_coin_business_websites(element):
    '''Take unstyled_list_element from the coinmarketcap link section.

    :param requests_html.Element:
    :return list of coin main website links:
    :rtype: [links]'''

    if element:
        return [link.absolute_links.pop() for link in element.find('a', containing='Website')]
    return None


def get_coin_telegram_pages(element):
    '''Take unstyled_list_element from the coinmarketcap link section.

    :param requests_html.Element:
    :return list of telegram links from coinmarketcap page.:
    :rtype: [links]'''

    if element:
        pre = [link.absolute_links.pop() for link in element.find('a', containing='Chat')]
        post = filter(is_valid_telegram_link, pre)
        return list(post)
    return list()


def parse_title_from_url(url):
    '''Parses currency name from url

    :param str url:
    :return currency name:
    :rtype: str'''

    title = urlparse(url)
    title = title.path.split('/')

    title = list(filter(None, title))
    title.remove('currencies')
    return title[0]


def parse_telegram_link(url):
    '''Parses telegram link from url

    :param str url:
    :return telegram links:
    :rtype: [str]'''

    html = fetch_page(url)
    if html:
        try:
            preprocessing = list(html.absolute_links)
            postfiltering = filter(is_valid_telegram_link, preprocessing)
            return list(postfiltering)
        except (UnicodeDecodeError, ValueError):
            print('T', end='', flush=True)
            return list()
    return list()


def parse_twitter_link(links):
    '''Parses twitter link from links

    :param list [links]:
    :return twitter links:
    :rtype: [str]'''

    if links:
        filtering = filter(is_valid_twitter_link, links)
        return list(filtering)
    return list()


def parse_business_links(links):
    temp = []
    if links:
        for link in links:
            parsed = parse_telegram_link(link)
            if len(parsed) > 1:
                temp += parsed

        return list(set(temp))
    return list()


def parse_coins_link_information(url):
    coin_data = defaultdict(None)

    if url:

        element, links = get_elements_and_links_from_url(url)

        title = parse_title_from_url(url)

        business_page = get_coin_business_websites(element)
        telegram_links = get_coin_telegram_pages(element)
        twitter_links = parse_twitter_link(links)

        if business_page:
            new_telegram_links = parse_business_links(business_page)
            if new_telegram_links:
                telegram_links = list(set(telegram_links + new_telegram_links))

        # coin_data.update({'business_page': business_page}) # no reason to keep this.
        coin_data.update({'telegram_links': telegram_links})
        coin_data.update({'twitter_links': twitter_links})
        coin_data.update({'name': title})
        print('.', end='', flush=True)

        if telegram_links == None:
            print(coin_data)

        return coin_data

    return coin_data
