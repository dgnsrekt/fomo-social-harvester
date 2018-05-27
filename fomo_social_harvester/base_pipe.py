# STANDARDLIB
from csv import DictWriter
from datetime import date
from pathlib import Path
from time import sleep
import json
import logging
import multiprocessing
from multiprocessing import Pool, cpu_count

# THIRD-PARTY
import luigi
import pandas as pd
import structlog
from structlog.stdlib import LoggerFactory


# LOCAL-APP
from constains import DATAPATH
from scraper.utils import timeit
from scraper.link_scraper import (get_coinmarketcap_links,
                                  parse_coins_link_information)


class CreateDataFolder(luigi.Task):

    def output(self):
        return luigi.LocalTarget(str(DATAPATH))

    def run(self):
        Path(self.output().path).mkdir(exist_ok=True)


class CreateDateFolder(luigi.Task):
    date = luigi.DateParameter(default=date.today())

    def requires(self):
        return CreateDataFolder()

    def output(self):
        datepath = Path(str(self.input().path)) / str(self.date)
        return luigi.LocalTarget(str(datepath))

    def run(self):
        Path(self.output().path).mkdir(exist_ok=True)


class ParseMetaDataToJSONTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    limit = luigi.parameter.IntParameter(default=None)

    def requires(self):
        return CreateDateFolder(date=self.date)

    def output(self):
        path = Path(str(self.input().path)) / 'Raw.json'
        return luigi.LocalTarget(str(path))

    def run(self):
        # TODO: DEBUG config option which changes limit to 99
        links_ = get_coinmarketcap_links(limit=self.limit)

        max_processes = cpu_count() * 2

        with Pool(max_processes) as p:
            records = p.map(parse_coins_link_information, links_)  # TODO: CHANGE

        if len(records) > 0:
            json_dump = json.dumps(records)
            with self.output().open('w') as _out:
                _out.write(json_dump)


class ParseTelegramJSONtoCSVTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    limit = luigi.parameter.IntParameter(default=None)

    def requires(self):
        return [CreateDateFolder(date=self.date), ParseMetaDataToJSONTask(date=self.date, limit=self.limit)]

    def output(self):
        path = Path(str(self.input()[0].path)) / 'Telegram.csv'
        return luigi.LocalTarget(str(path))

    def run(self):
        df = pd.read_json(self.input()[1].path)
        df.set_index('name', inplace=True)

        csv_file = self.output().open('w')

        with csv_file:
            fnames = ['name', 'telegram']
            writer = DictWriter(csv_file, fieldnames=fnames)
            writer.writeheader()

            for row in df.iterrows():
                name = row[0]
                for link in row[1]['telegram_links']:
                    csv_row = {'name': name, 'telegram': link}
                    writer.writerow(csv_row)


class ParseTwitterJSONtoCSVTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    limit = luigi.parameter.IntParameter(default=None)

    def requires(self):
        return [CreateDateFolder(date=self.date), ParseMetaDataToJSONTask(date=self.date, limit=self.limit)]

    def output(self):
        path = Path(str(self.input()[0].path)) / 'Twitter.csv'
        return luigi.LocalTarget(str(path))

    def run(self):
        df = pd.read_json(self.input()[1].path)
        df.set_index('name', inplace=True)

        csv_file = self.output().open('w')

        with csv_file:
            fnames = ['name', 'twitter']
            writer = DictWriter(csv_file, fieldnames=fnames)
            writer.writeheader()

            for row in df.iterrows():
                name = row[0]
                for link in row[1]['twitter_links']:
                    csv_row = {'name': name, 'twitter': link}
                    writer.writerow(csv_row)


if __name__ == '__main__':
    task_one = ParseTelegramJSONtoCSVTask(limit=99)
    task_two = ParseTwitterJSONtoCSVTask(limit=99)
    luigi.build([task_one, task_two], local_scheduler=True)
