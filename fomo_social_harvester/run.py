import functools
from time import sleep
from datetime import date

import schedule
import luigi

from scraper.utils import get_current_hour
from telegram_pipe import TelegramMembersToDatabaseTask
from twitter_pipe import TwitterMembersToDatabaseTask


class SocialHarvestTask(luigi.WrapperTask):
    date = luigi.DateParameter(default=date.today())
    hour = luigi.DateHourParameter(default=get_current_hour())
    debug = luigi.BoolParameter(default=False)

    def requires(self):
        yield TelegramMembersToDatabaseTask(date=self.date, hour=self.hour, debug=self.debug)
        yield TwitterMembersToDatabaseTask(date=self.date, hour=self.hour, debug=self.debug)


def with_logging(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        print('LOG: Running job "%s"' % func.__name__)
        result = func(*args, **kwargs)
        print('LOG: Job "%s" completed' % func.__name__)
        return result
    return wrapper


@with_logging
def job():
    date_ = date.today()
    hour_ = get_current_hour()
    debug_ = False
    luigi.build([SocialHarvestTask(date=date_, hour=hour_, debug=debug_)], workers=2)


def main():
    for t in range(24):
        d = f'{t:02d}:5'
        schedule.every().day.at(d).do(job)

    while True:
        schedule.run_pending()
        til_next = int(schedule.idle_seconds()/60)
        if til_next % 10 == 0:
            print(f'Next Task running in: {til_next} minutes')
        sleep(1)


if __name__ == '__main__':
    job()
    main()
