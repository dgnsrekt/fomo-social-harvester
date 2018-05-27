from fomo_social_harvester.scraper import base


def test_import():
    pass


def test_fetch_bad_page():
    assert base.fetch_page('www') == None
