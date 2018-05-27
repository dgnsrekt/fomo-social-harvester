from fomo_social_harvester.scraper import link_scraper as link
from urllib.parse import urlparse


def test_import():
    pass


def test_parse_maxpages():
    assert isinstance(link.parse_maxpages(), int)
    assert link.parse_maxpages() > 1


def test_get_coinmarketcap_links():
    links = link.get_coinmarketcap_links(limit=99)
    assert len(links) == 99
    assert isinstance(links[0], str)
    assert isinstance(links[-1], str)

    url = urlparse(links[0])
    assert url.scheme == 'https'

    url = urlparse(links[1])
    assert url.scheme == 'https'
