# STANDARDLIB
from csv import DictWriter
from datetime import date
import logging
from multiprocessing import Pool, cpu_count
from time import sleep
import json

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


structlog.configure(logger_factory=LoggerFactory())
logging.basicConfig(level='INFO')


class ParseMetaDataToJSONTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())

    def output(self):
        path = str(DATAPATH / f'{self.date}.json')
        return luigi.LocalTarget(path)

    def run(self):
        links_ = get_coinmarketcap_links(limit=None)  # TODO: DEBUG config option

        max_processes = cpu_count() * 2

        with Pool(max_processes) as p:
            records = p.map(parse_coins_link_information, links_)

        if len(records) > 0:
            json_dump = json.dumps(records)
            with self.output().open('w') as _out:
                _out.write(json_dump)


class ParseTelegramJSONtoCSVTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())

    def requires(self):
        return ParseMetaDataToJSONTask()

    def output(self):
        path = str(DATAPATH / f'{self.date}_Telegram.csv')
        return luigi.LocalTarget(path)

    def run(self):
        df = pd.read_json(self.input().path)
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

    def requires(self):
        return ParseMetaDataToJSONTask()

    def output(self):
        path = str(DATAPATH / f'{self.date}_Twitter.csv')
        return luigi.LocalTarget(path)

    def run(self):
        df = pd.read_json(self.input().path)
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
    task_one = ParseTelegramJSONtoCSVTask()
    task_two = ParseTwitterJSONtoCSVTask()
    luigi.build([task_one, task_two], local_scheduler=True)
