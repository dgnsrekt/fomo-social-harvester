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
    html = fetch_page(telegram_link)
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

    print(telegram_link)

    return {'name': name, 'members': None}
