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
import base_pipe
from constains import DATAPATH
from scraper.telegram import parse_member_count
from scraper.utils import get_current_hour
from models.telegram import Telegram, create_telegram_table, clean_telegram_table

# structlog.configure(logger_factory=LoggerFactory())
# logging.basicConfig(level='INFO')


class ParseTelegramMemberCountTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    hour = luigi.DateHourParameter(default=get_current_hour())
    limit = luigi.Parameter(default=None)  # DEBUG: REMOVE THIS!!!!

    def requires(self):
        return [base_pipe.CreateDateFolder(), base_pipe.ParseTelegramJSONtoCSVTask()]

    def output(self):
        path = Path(str(self.input()[0].path)) / f'Telegram_Data_{self.hour}.csv'
        return luigi.LocalTarget(str(path))

    def run(self):
        telegram_links = []

        with self.input()[1].open('r') as f:
            reader = csv.reader(f)
            header = next(reader)

            for i, row in enumerate(reader):
                name = row[0]
                link = row[1]
                telegram_links.append({'name': name, 'link': link})  # IDEA: tuple
                if self.limit:
                    if i > self.limit:
                        break

        max_processes = cpu_count() * 2

        with Pool(max_processes) as p:
            member_records = p.map(parse_member_count, telegram_links)

        if len(member_records) > 0:
            df = pd.DataFrame(member_records)
            df.set_index('name', inplace=True)

            mean_series = df.groupby(df.index)['members'].mean()

            sum_series = df.groupby(df.index)['members'].sum()

            median_series = df.groupby(df.index)['members'].median()

            count_series = df.groupby(df.index).count()

            data = pd.concat([sum_series,
                              mean_series,
                              median_series,
                              count_series], axis=1)
            data.columns = ['sum', 'mean', 'median', 'link_count']
            data.dropna(inplace=True)
            data.to_csv(self.output().path)


class TelegramMembersToDatabaseTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    hour = luigi.DateHourParameter(default=get_current_hour())
    debug = luigi.BoolParameter(default=False)  # NOTE: SUPER DANGEROUS WILL SCRUB DATABASE

    def requires(self):
        return ParseTelegramMemberCountTask()

    def run(self):
        if not Telegram.table_exists():
            create_telegram_table()

        if not self.complete():

            df = pd.read_csv(self.input().path)
            df.set_index('name', inplace=True)
            for name, row in df.round(2).iterrows():
                _mean = row['mean']
                _median = row['median']
                _sum = row['sum']
                _count = row['link_count']

                data = {'name': name, 'mean': int(_mean), 'median': int(_median),
                        'sum': int(_sum), 'count': int(_count), 'date': self.hour}
                Telegram.add_member_data(**data)

            # TODO: TELEGRAM LINKS, RAW DATA. rename csv files

    def complete(self):
        # TODO: Add task to create a DB/Table or
        # IDEA: Add an except for no table - create table then check databsse for complete
        if self.debug:
            clean_telegram_table()  # DEBUG: REMOVE
            print('DELETING TABLE FOR DEBUGGING!!!!!!!!!!!!!!!!!')
        try:
            local_ = pd.read_csv(self.input().path)
            dbase_ = Telegram.data_by_date(self.hour)
            print('#' * 25)
            print(len(local_))  # TODO: Logging
            print(len(dbase_))  # TODO: Logging
            # TODO: If else raise data not written to db
            print(len(local_.index) == len(dbase_.index))
            # TODO: If else raise data not written to db
            print('#' * 25)
            return len(local_.index) == len(dbase_.index)

        except (FileNotFoundError, KeyError) as e:
            print()
            return False


def job():
    print('running job..')
    task = TelegramMembersToDatabaseTask(debug=False)  # TODO: remeove this
    luigi.build([task], local_scheduler=True)
    # luigi.build([task])


if __name__ == '__main__':
    job()

    # schedule.every().hour.at(':00').do(job)
    # print('job scheduled')
    #
    # while True:
    #     schedule.run_pending()
    #     print(schedule.idle_seconds())
    #     sleep(1)
