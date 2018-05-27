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
        return None


def parse_following(element):
    following_selector = 'li.ProfileNav-item.ProfileNav-item--following > a > span.ProfileNav-value'
    try:
        return int(element.find(following_selector)[0].element.values()[1])
    except IndexError:
        return None


def parse_followers(element):
    followers_selector = 'li.ProfileNav-item.ProfileNav-item--followers > a > span.ProfileNav-value'
    try:
        return int(element.find(followers_selector)[0].element.values()[1])
    except IndexError:
        return None


def parse_likes(element):
    likes_selector = 'li.ProfileNav-item.ProfileNav-item--favorites > a > span.ProfileNav-value'
    try:
        return int(element.find(likes_selector)[0].element.values()[1])
    except IndexError:
        return None


def parse_twitter_count(row):
    sleep(5)
    name = row.get('name')
    twitter_link = row.get('link')
    html = fetch_page(twitter_link)

    selector = '#page-container > div.ProfileCanopy.ProfileCanopy--withNav.ProfileCanopy--large.js-variableHeightTopBar > div > div.ProfileCanopy-navBar.u-boxShadow > div > div > div.Grid-cell.u-size2of3.u-lg-size3of4 > div > div > ul'
    if html:

        try:
            element = html.find(selector)[0]

        except IndexError:
            print('I', end='', flush=True)
            return {'name': name, 'tweets': None, 'following': None,
                    'followers': None, 'likes': None}

        else:
            # TODO: MAY NEED TO CHANGE TO RAW ELEMENT
            tweets = parse_tweets(element)
            following = parse_following(element)
            followers = parse_followers(element)
            likes = parse_likes(element)

            print('.', end='', flush=True)
            return {'name': name, 'tweets': tweets, 'following': following,
                    'followers': followers, 'likes': likes}
