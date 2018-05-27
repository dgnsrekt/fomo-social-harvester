from constains import BAD_TWITTER_LINKS
import logging


class Twitter:

    def get_bad_twitter_links():
        with open(BAD_TWITTER_LINKS, 'r') as f:
            return f.read().split('\n')[:-1]

    bad_twitter_links = get_bad_twitter_links()

    @classmethod
    def is_link_bad(cls, link):
        if link in cls.bad_twitter_links:
            logging.info(f'Bad Link Found: {link}')  # TODO Turn to structlog
        return link in cls.bad_twitter_links


# print(Twitter.is_link_bad('https://twitter.com/startaico'))
# print(Twitter.is_link_bad('kevin'))
