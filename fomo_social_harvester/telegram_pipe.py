# STANDARDLIB
import csv
from csv import DictWriter
from datetime import date
import logging
from multiprocessing import Pool, cpu_count
from time import sleep
import json

# THIRD-PARTY
import luigi
import luigi.contrib.postgres
import pandas as pd
import structlog
from structlog.stdlib import LoggerFactory

# LOCAL-APP
from constains import DATAPATH
from scraper.telegram import parse_member_count
from scraper.utils import get_current_hour
from models.telegram import Telegram, create_telegram_table, clean_telegram_table
import link_pipe

structlog.configure(logger_factory=LoggerFactory())
logging.basicConfig(level='INFO')


class InputTelegramCSV(luigi.ExternalTask):
    date = luigi.DateParameter(default=date.today())

    def output(self):
        path = str(DATAPATH / f'{self.date}_Telegram.csv')
        return luigi.LocalTarget(path)


class ParseTelegramMemberCountTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    hour = luigi.DateHourParameter(default=get_current_hour())
    limit = luigi.Parameter(default=None)

    def output(self):
        path = str(DATAPATH / f'{self.hour}_TelegramMembers.csv')
        return luigi.LocalTarget(path)

    def requires(self):
        return link_pipe.ParseTelegramJSONtoCSVTask()

    def run(self):
        telegram_links = []

        with self.input().open('r') as f:
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
        if self.debug:
            clean_telegram_table()  # DEBUG: REMOVE
        if not Telegram.table_exists():
            create_telegram_table()

        df = pd.read_csv(self.input().path)
        df.set_index('name', inplace=True)
        for name, row in df.round(2).iterrows():
            _mean = row['mean']
            _median = row['median']
            _sum = row['sum']
            _count = row['link_count']

            data = {'name': name, 'mean': _mean, 'median': _median,
                    'sum': int(_sum), 'count': int(_count), 'date': self.hour}
            Telegram.add_member_data(**data)

            # TODO: TELEGRAM LINKS, RAW DATA. rename csv files

    def complete(self):
        try:
            local_ = pd.read_csv(self.input().path)
            dbase_ = Telegram.data_by_date(self.hour)
            print(len(local_))  # TODO: Logging
            print(len(dbase_))  # TODO: Logging
            # TODO: If else raise data not written to db
            print(len(local_.index) == len(dbase_.index))
            # TODO: If else raise data not written to db
            return len(local_.index) == len(dbase_.index)
        except FileNotFoundError:
            return False


if __name__ == '__main__':
    task = TelegramMembersToDatabaseTask()
    # luigi.build([task], local_scheduler=True)
    luigi.build([task])
