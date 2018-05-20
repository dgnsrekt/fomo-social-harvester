import datetime

from peewee import PostgresqlDatabase, Model

# TODO: add a config file that sets the database name
db = PostgresqlDatabase('FOMO_SOCIAL_HARVESTER')


def get_current_date_time():
    return datetime.utcnow()


class BaseModel(Model):

    class Meta:
        database = db
