import functools
from time import sleep

import schedule
import luigi

from telegram_pipe import TelegramMembersToDatabaseTask
from twitter_pipe import TwitterMembersToDatabaseTask


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
    debug = False
    task_a = TwitterMembersToDatabaseTask(debug=debug)
    task_b = TelegramMembersToDatabaseTask(debug=debug)

    tasks = [task_a, task_b]
    luigi.build(tasks)


def main():
    for t in range(24):
        d = f'{t:02d}:10'
        schedule.every().day.at(d).do(job)

    while True:
        schedule.run_pending()
        til_next = int(schedule.idle_seconds()/60)
        if til_next % 10 == 0:
            print(f'Next Task running in: {til_next} minutes')
        sleep(1)


if __name__ == '__main__':
    main()
