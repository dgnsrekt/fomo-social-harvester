from time import sleep
import logging

from .base import fetch_page


def clean_member_count(string):
    cleaned = string.replace('members', '')
    cleaned = cleaned.replace(' ', '')
    return int(cleaned)


def parse_member_count(row):
    sleep(5)
    name = row.get('name')
    telegram_link = row.get('link')

    telegram_header = {
        'Accept': 'text/html, application/xhtml+xml, application/xml; q=0.9, image/webp,image/apng, */*; q=0.8',
        'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0.3359.181 Safari/537.36'
    }

    html = fetch_page(telegram_link, header=telegram_header)
    text = html.text.split('\n')

    for line in text:
        if 'members' in line:
            try:
                cleaned = clean_member_count(line)
            except ValueError:
                cleaned = None
            logging.debug('f{cleaned} members found')
            print('.', end='', flush=True)
            if cleaned:
                return {'name': name, 'members': cleaned}

    return {'name': name, 'members': None}
