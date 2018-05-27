# STANDARDLIB
import logging
import pandas as pd

# THRID-PARTY
from peewee import *

# LOCAL-APP
from .base import BaseModel, db, get_current_date_time


class Twitter(BaseModel):
    name = CharField(null=False)

    followers = BigIntegerField(null=True)
    following = BigIntegerField(null=True)
    likes = BigIntegerField(null=True)
    tweets = BigIntegerField(null=True)

    date = DateTimeField()

    def add_member_data(**kwargs):
        print(':', end='', flush=True)

        Twitter.create(**kwargs)
        logging.debug(f'{kwargs} added.')

    def data_by_date(date):
        try:
            query = Twitter.select(Twitter.name,
                                   Twitter.followers,
                                   Twitter.following,
                                   Twitter.likes,
                                   Twitter.tweets
                                   ).where(Twitter.date == date).dicts()
            df = pd.DataFrame(list(query))
            df.set_index('name', inplace=True)
            return df
        except Exception as e:
            print(e)
            raise

    def query_all():
        query = Twitter.select(Twitter.date,
                               Twitter.name,
                               Twitter.followers,
                               Twitter.following,
                               Twitter.likes,
                               Twitter.tweets
                               ).dicts()

        df = pd.DataFrame(list(query))
        df.set_index('date', inplace=True)
        columns = ['name',
                   'followers',
                   'following',
                   'likes',
                   'tweets']
        return df[columns]


def create_twitter_table():
    db.create_tables([Twitter])
    logging.info('Twitter Table Created')


def drop_twitter_table():
    db.drop_tables([Twitter])
    logging.info('Twitter Table Dropped')


def clean_twitter_table():
    drop_twitter_table()
    create_twitter_table()
