from time import sleep
import logging

from .base import fetch_page


class TwitterParsingError(Exception):
    pass


def parse_tweets(element):
    tweet_selector = 'li.ProfileNav-item.ProfileNav-item--tweets.is-active > a > span.ProfileNav-value'
    try:
        return int(element.find(tweet_selector)[0].element.values()[1])
    except IndexError:
        print('TW', end='', flush=True)
        return 0


def parse_following(element):
    following_selector = 'li.ProfileNav-item.ProfileNav-item--following > a > span.ProfileNav-value'
    try:
        return int(element.find(following_selector)[0].element.values()[1])
    except IndexError:
        print('FG', end='', flush=True)
        return 0


def parse_followers(element):
    followers_selector = 'li.ProfileNav-item.ProfileNav-item--followers > a > span.ProfileNav-value'
    try:
        return int(element.find(followers_selector)[0].element.values()[1])
    except IndexError:
        print('FR', end='', flush=True)
        return 0


def parse_likes(element):
    likes_selector = 'li.ProfileNav-item.ProfileNav-item--favorites > a > span.ProfileNav-value'
    try:
        return int(element.find(likes_selector)[0].element.values()[1])
    except IndexError:
        print('L', end='', flush=True)
        return 0


def parse_twitter_count(row):
    sleep(.1)
    name = row.get('name')
    twitter_link = row.get('link')

    user = twitter_link.split('/')[-1]

    twitter_header = {
        'Accept': 'application/json, text/javascript, */*; q=0.01',
        'Referer': f'https://twitter.com/{user}',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/603.3.8 (KHTML, like Gecko) Version/10.1.2 Safari/603.3.8',
        'X-Twitter-Active-User': 'yes',
        'X-Requested-With': 'XMLHttpRequest'
    }

    html = fetch_page(twitter_link, header=twitter_header)
    # html = fetch_page(twitter_link)

    selector_one = '#page-container > div.ProfileCanopy.ProfileCanopy--withNav.ProfileCanopy--large.js-variableHeightTopBar > div > div.ProfileCanopy-navBar.u-boxShadow > div > div > div.Grid-cell.u-size2of3.u-lg-size3of4 > div > div > ul'
    selector_two = '#page-container > div.ProfileCanopy.ProfileCanopy--withNav.js-variableHeightTopBar > div > div.ProfileCanopy-navBar.u-boxShadow > div > div > div.Grid-cell.u-size2of3.u-lg-size3of4 > div > div > ul'

    if html:
        if html.url == 'https://twitter.com/account/suspended':
            print(name, 'suspended')
            return {'name': name, 'tweets': None, 'following': None,
                    'followers': None, 'likes': None}

        element_one = html.find(selector_one)
        element_two = html.find(selector_two)

        if len(element_one) > 0:
            element = element_one
        elif len(element_two) > 0:
            element = element_two
        else:
            print(html, name, 'Element Not Found')
            raise TwitterParsingError(f'Element Not found {name}')

        # try:
        element = element[0]

        # except IndexError:
        #     print(name, element, end='', flush=True)
        #     return {'name': name, 'tweets': None, 'following': None,
        #             'followers': None, 'likes': None}
        #
        # else:
        tweets = parse_tweets(element)
        following = parse_following(element)
        followers = parse_followers(element)
        likes = parse_likes(element)

        print('.', end='', flush=True)
        return {'name': name, 'tweets': tweets, 'following': following,
                'followers': followers, 'likes': likes}
    print()
    print(f'Oops! Either "{user}" does not exist or is private.')
    return {'name': name, 'tweets': 0, 'following': 0,
            'followers': 0, 'likes': 0}
