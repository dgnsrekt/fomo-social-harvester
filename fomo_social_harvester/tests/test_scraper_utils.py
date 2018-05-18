from fomo_social_harvester.scraper import utils


def test_is_valid_telegram_link():
    url = 'https://t.me/joinchat/AAAAAEQbOeucnaMWN0A9dQ'
    assert utils.is_valid_telegram_link(url) == True

    url = 'https://t.me/joinchat/GCeNfxAVLy6bo6VNR3a9hg'
    assert utils.is_valid_telegram_link(url) == True

    url = 'https://t.me/lendroidproject'
    assert utils.is_valid_telegram_link(url) == True

    url = 'https://t.me/joinchat/HVP0kBBx-murObE3hpv7HA'
    assert utils.is_valid_telegram_link(url) == True

    url = 'https://t.me/RaiWalletBot'
    assert utils.is_valid_telegram_link(url) == True

    url = 'https://t.me/StratisPlatform'
    assert utils.is_valid_telegram_link(url) == True

    url = 'https://t.me/joinchat/AAAAAEPriD4KXShTKx-Kpg'
    assert utils.is_valid_telegram_link(url) == True

    url = 'https://t.me/intelligenttradingbot'  # bad data
    assert utils.is_valid_telegram_link(url) == True

    url = 'https://t.me/intelligenttrading'
    assert utils.is_valid_telegram_link(url) == True


def test_is_valid_twitter_link():
    url = 'https://twitter.com/EOS_io'
    assert utils.is_valid_twitter_link(url) == True

    url = 'https://twitter.com/SiaTechHQ'
    assert utils.is_valid_twitter_link(url) == True

    url = 'https://twitter.com/nanocurrency'
    assert utils.is_valid_twitter_link(url) == True

    url = 'https://twitter.com/vergecurrency'
    assert utils.is_valid_twitter_link(url) == True
