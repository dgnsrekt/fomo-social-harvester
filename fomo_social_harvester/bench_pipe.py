# STANDARDLIB
from csv import DictWriter
import logging
import json
from pathlib import Path
from multiprocessing import Pool, cpu_count
from time import sleep

# THIRD-PARTY
import luigi
import pandas as pd
import structlog
from structlog.stdlib import LoggerFactory


# LOCAL-APP
from constains import BENCHPATH
from plotly.offline import plot
from plotly.graph_objs import Scatter
from scraper.link_scraper import (get_coinmarketcap_links,
                                  parse_coins_link_information)
#
from scraper.telegram import parse_member_count
from scraper.twitter import parse_twitter_count
#
from base_pipe import ParseTwitterJSONtoCSVTask


class CreateBENCHFolder(luigi.Task):

    def output(self):
        return luigi.LocalTarget(str(BENCHPATH))

    def run(self):
        Path(self.output().path).mkdir(exist_ok=True)


class ParseMetaDataToJSONTask(luigi.Task):
    def requires(self):
        return CreateBENCHFolder()

    def output(self):
        path = Path(str(self.input().path)) / 'Raw.json'
        return luigi.LocalTarget(str(path))

    def run(self):
        links_ = get_coinmarketcap_links(limit=99)

        max_processes = cpu_count() * 2

        with Pool(max_processes) as p:
            records = p.map(parse_coins_link_information, links_)

        if len(records) > 0:
            json_dump = json.dumps(records)
            with self.output().open('w') as _out:
                _out.write(json_dump)


class ParseTelegramJSONtoCSVTask(luigi.Task):
    def requires(self):
        return [CreateBENCHFolder(), ParseMetaDataToJSONTask()]

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

    def requires(self):
        return [CreateBENCHFolder(), ParseMetaDataToJSONTask()]

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
    task_one = ParseTelegramJSONtoCSVTask()
    task_two = ParseTwitterJSONtoCSVTask()
    luigi.build([task_one, task_two], local_scheduler=True)
