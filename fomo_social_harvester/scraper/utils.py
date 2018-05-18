
import functools
from time import time

from requests.exceptions import (SSLError, ReadTimeout, ConnectTimeout,
                                 ConnectionError, ChunkedEncodingError)


def scraper_exception_handler():
    """
    A decorator that wraps the passed in function and logs
    exceptions should one occur

    @param logger: The logging object
    """

    def decorator(func):

        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except(SSLError,
                    ReadTimeout,
                    ConnectTimeout,
                    ConnectionError,
                    ChunkedEncodingError,
                    UnicodeDecodeError,
                    ValueError) as e:
                print('E', end='', flush=True)

            except Exception as e:
                print('X', end='', flush=True)
                raise

        return wrapper
    return decorator


def is_valid_telegram_link(link):
    '''Checks link to see if its a real telegram link.

    :param str link:
    :return True if valid telegram link:
    :rtype: bool'''

    if 'https://t.me/' in link:
        return True
    elif 'https://telegram.me/' in link:
        return True
    elif 'http://www.telegram.me/' in link:
        return True
    elif 'http://t.me/' in link:
        return True
    else:
        return False


def is_valid_twitter_link(link):
    if 'https://twitter.com/CoinMarketCap' in link:
        return False

    elif 'https://twitter.com' in link:
        return True
    elif 'http://twitter.com' in link:
        return True
    else:
        return False


def timeit(method):
    def timed(*args, **kw):
        ts = time()
        result = method(*args, **kw)
        te = time()
        time_result = ((te - ts) * 1000) / 60
        print(f'{method.__name__.upper()} Completed in: {time_result: 2.2f} s')
        return time_result, result
    return timed
