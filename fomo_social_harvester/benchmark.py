# STANDARDLIB
import logging
from multiprocessing import Pool, cpu_count
from time import sleep

# THIRD-PARTY
import structlog
from structlog.stdlib import LoggerFactory

# LOCAL-APP
from plotly.offline import plot
from plotly.graph_objs import Scatter
from scraper.utils import timeit
from scraper.link_scraper import (get_coinmarketcap_links,
                                  parse_coins_link_information)


@timeit
def parse_all_coin_data(max_processes=None, limit=None):
    # TODO: export to benchmarking
    coin_links = get_coinmarketcap_links(limit=limit)
    if max_processes == None:
        max_processes = cpu_count * 2

    with Pool(max_processes) as p:
        records = p.map(parse_coins_link_information, coin_links)

    if len(records) > 0:
        print(f'\nrecords found {len(records)}')
        return records

    # print('done')


def benchmark_link_scraper(limit=None):
    import pandas as pd
    # TODO: export to benchmarking
    structlog.configure(logger_factory=LoggerFactory())
    logging.basicConfig(level='CRITICAL')
    center = '*' * 10

    print(center, 'Link Scraper Processor Benchmark', center)

    time_list = []
    min_processors = 2
    max_processors = (cpu_count() * 2) + 1

    for processor in range(min_processors, max_processors):
        print()
        print(center, f'processors: {processor}', center)
        t, r = parse_all_coin_data(max_processes=processor, limit=limit)
        time_list.append({'time': t, 'processors': processor})
        sleep(2)

    data = pd.DataFrame(time_list)
    trace = Scatter(x=data.processors, y=data.time)
    plot([trace])
    print(data)


if __name__ == '__main__':
    benchmark_link_scraper(limit=99, auto_open=False)
