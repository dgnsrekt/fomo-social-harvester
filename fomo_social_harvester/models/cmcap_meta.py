import pandas as pd
import logging

from peewee import *
from fomo_social_harvester.models.base_model import BaseModel, db, get_current_date_time


class MetaData(BaseModel):
    name = CharField(null=False)
    date_created = DateTimeField()

    def add_data(**kwargs):
        try:
            kwargs['date_created'] = get_current_date_time()
            MetaData.create(**kwargs)
            logging.info(f'{kwargs.name} added.')
        except IntegrityError:
            db.rollback()
            logging.info(f'{kwargs.name} already exists')


class TelegramLinks(BaseModel):
    name = ForeignKeyField(MetaData, to_field='name')
    telegram = CharField(null=False, unique=True)
    date_created = DateTimeField()

    def add_data(**kwargs):
        try:
            kwargs['date_created'] = get_current_date_time()
            TelegramLinks.create(**kwargs)
            logging.info(f'{kwargs.name} added.')
        except IntegrityError:
            db.rollback()
            logging.info(f'{kwargs.name} already exists')


class TwitterLinks(BaseModel):
    name = ForeignKeyField(MetaData, to_field='name')
    twitter = CharField(null=False, unique=True)
    date_created = DateTimeField()

    def add_data(**kwargs):
        try:
            kwargs['date_created'] = get_current_date_time()
            TwitterLinks.create(**kwargs)
            logging.info(f'{kwargs.name} added.')
        except IntegrityError:
            db.rollback()
            logging.info(f'{kwargs.name} already exists')


def create_meta_data_tables():
    db.create_tables([MetaData,
                      TelegramLinks,
                      TwitterLinks
                      ])
    logging.info('CMCapMetaData Tables Created')


def drop_meta_data_tables():
    db.drop_tables([MetaData,
                    TelegramLinks,
                    TwitterLinks
                    ])
    logging.info('CMCapMetaData Tables Dropped')


def clean_cmcap_meta_data_tables():
    drop_meta_data_tables()
    create_meta_data_tables()
