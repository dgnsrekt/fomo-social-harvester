# STANDARDLIB
import csv
from csv import DictWriter
from datetime import date
import logging
from multiprocessing import Pool, cpu_count
from pathlib import Path
from time import sleep
import json

# THIRD-PARTY
import luigi
import luigi.contrib.postgres
import pandas as pd
import schedule
import structlog
from structlog.stdlib import LoggerFactory
from peewee import ProgrammingError

# LOCAL-APP
import bad_links
import base_pipe
from constains import DATAPATH
from scraper import twitter
from scraper.utils import get_current_hour
from models.twitter import Twitter, create_twitter_table, clean_twitter_table

structlog.configure(logger_factory=LoggerFactory())
logging.basicConfig(level='INFO')


class ParseTwitterMemberCountTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    hour = luigi.DateHourParameter(default=get_current_hour())
    limit = luigi.Parameter(default=None)

    def requires(self):
        return [base_pipe.CreateDateFolder(date=self.date), base_pipe.ParseTwitterJSONtoCSVTask(date=self.date)]

    def output(self):
        path = Path(str(self.input()[0].path)) / f'Twitter_Data_{self.hour}.csv'
        return luigi.LocalTarget(str(path))

    def run(self):
        twitter_links = []

        with self.input()[1].open('r') as f:
            reader = csv.reader(f)
            header = next(reader)

            for i, row in enumerate(reader):
                name = row[0]
                link = row[1]
                if not bad_links.Twitter.is_link_bad(link):
                    twitter_links.append({'name': name, 'link': link})  # IDEA: tuple
                if self.limit:
                    if i > self.limit:
                        break

        max_processes = cpu_count() * 2
        print(f'parsing {len(twitter_links)} twitter links')

        with Pool(max_processes) as p:
            member_records = p.map(twitter.parse_twitter_count, twitter_links)

        if len(member_records) > 0:
            df = pd.DataFrame(member_records)
            df.set_index('name', inplace=True)
            df.to_csv(self.output().path)


class TwitterMembersToDatabaseTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    hour = luigi.DateHourParameter(default=get_current_hour())
    debug = luigi.BoolParameter(default=False)  # NOTE: SUPER DANGEROUS WILL SCRUB DATABASE

    def requires(self):
        return ParseTwitterMemberCountTask(date=self.date, hour=self.hour)

    def run(self):
        if not Twitter.table_exists():
            create_twitter_table()

        if not self.complete():

            df = pd.read_csv(self.input().path)
            df.set_index('name', inplace=True)
            for name, row in df.iterrows():
                followers = row['followers']
                following = row['following']
                likes = row['likes']
                tweets = row['tweets']

                data = {'name': name, 'followers': followers, 'following': following, 'likes': likes,
                        'tweets': tweets, 'date': self.hour}
                Twitter.add_member_data(**data)

        # TODO: Twitter LINKS, RAW DATA. rename csv files

    def complete(self):
        # TODO: Add task to create a DB/Table or
        # IDEA: Add an except for no table - create table then check databsse for complete
        if self.debug:
            clean_twitter_table()  # DEBUG: REMOVE
            print('DELETING TABLE FOR DEBUGGING!!!!!!!!!!!!!!!!!')
        try:
            local_ = pd.read_csv(self.input().path)
            dbase_ = Twitter.data_by_date(self.hour)
            print('#' * 25)
            print(len(local_))  # TODO: Logging
            print(len(dbase_))  # TODO: Logging
            # TODO: If else raise data not written to db
            print(len(local_.index) == len(dbase_.index))
            # TODO: If else raise data not written to db
            print('#' * 25)
            return len(local_.index) == len(dbase_.index)

        except (FileNotFoundError, KeyError):
            print()
            return False


def job():
    print('running job..')
    task = TwitterMembersToDatabaseTask(debug=False)
    luigi.build([task], local_scheduler=True)


if __name__ == '__main__':
    job()
