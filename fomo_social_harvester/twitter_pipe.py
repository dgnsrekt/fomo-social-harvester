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
import schedule
import structlog
from structlog.stdlib import LoggerFactory

# LOCAL-APP
from constains import DATAPATH
# from scraper.telegram import parse_member_count
from scraper.utils import get_current_hour
# from models.telegram import Telegram, create_telegram_table, clean_telegram_table
import link_pipe

structlog.configure(logger_factory=LoggerFactory())
logging.basicConfig(level='INFO')


class ParseTwitterMemberCountTask(luigi.Task):
    date = luigi.DateParameter(default=date.today())
    hour = luigi.DateHourParameter(default=get_current_hour())
    limit = luigi.Parameter(default=None)

    def output(self):
        path = str(DATAPATH / f'{self.hour}_TwitterMembers.csv')
        return luigi.LocalTarget(path)

    def requires(self):
        return base_pipe.ParseTwitterJSONtoCSVTask()

    def run(self):
        df = pd.read_json(self)
