import pandas as pd
import logging

from peewee import *
from models.base import BaseModel, db, get_current_date_time


class Telegram(BaseModel):
    name = CharField(null=False)
    sum = DecimalField(max_digits=12, decimal_places=2, null=True)
    mean = DecimalField(max_digits=12, decimal_places=2, null=True)
    median = DecimalField(max_digits=12, decimal_places=2, null=True)
    count = BigIntegerField(null=True)

    date = DateTimeField()

    def add_member_data(**kwargs):
        print('.', end='', flush=True)

        Telegram.create(**kwargs)
        logging.debug(f'{kwargs} added.')

    def data_by_date(date):
        try:
            query = Telegram.select(Telegram.name,
                                    Telegram.sum,
                                    Telegram.mean,
                                    Telegram.median,
                                    Telegram.count
                                    ).where(Telegram.date == date).dicts()
            df = pd.DataFrame(list(query))
            df.set_index('name', inplace=True)
            return df

        except Exception as e:
            print(e)
            raise

    def query_all():
        query = Telegram.select(Telegram.date,
                                Telegram.name,
                                Telegram.sum,
                                Telegram.mean,
                                Telegram.median,
                                Telegram.count).dicts()

        df = pd.DataFrame(list(query))
        df.set_index('date', inplace=True)
        columns = ['name',
                   'mean',
                   'median',
                   'sum',
                   'count']
        return df[columns]


def create_telegram_table():
    db.create_tables([Telegram])
    logging.info('Telegram Table Created')


def drop_telegram_table():
    db.drop_tables([Telegram])
    logging.info('Telegram Table Dropped')


def clean_telegram_table():
    drop_telegram_table()
    create_telegram_table()