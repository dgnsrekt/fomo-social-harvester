# STANDARDLIB
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
        links_ = get_coinmarketcap_links(limit=99)  # TODO: DEBUG config option

        max_processes = cpu_count() * 2

        with Pool(max_processes) as p:
            records = p.map(parse_coins_link_information, links_)

        if len(records) > 0:
            json_dump = json.dumps(records)
            with self.output().open('w') as _out:
                _out.write(json_dump)


class ExtractJSONLoadDatabase(luigi.Task):
    date = luigi.DateParameter(default=date.today())

    def requires(self):
        path = str(DATAPATH / f'{self.date}.json')
        return luigi.LocalTarget(path)

    def run(self):
        links_ =


if __name__ == '__main__':
    luigi.build([ParseMetaDataToJSONTask()], local_scheduler=True)
