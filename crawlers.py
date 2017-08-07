#!/usr/bin/python3
import datetime
from io import BytesIO
from multiprocessing import Pool, Semaphore
import urllib.request
# import asyncio
import re
import gzip
import time
import lxml
import logging
import log
import settings
import sitemap
import parsers
import database
import robots
# from robots import RobotsTxt


def _get_content(url, timeout=60):
    print('_get_content: %s loading ...', url)
    try:
        rd = urllib.request.urlopen(url, timeout=timeout)
    except Exception as e:
        logging.error('_get_content (%s) exception %s', url, e)
        return ""

    content = ""
    if url.strip().endswith('.gz'):
        mem = BytesIO(rd.read())
        mem.seek(0)
        f = gzip.GzipFile(fileobj=mem, mode='rb')
        content = f.read().decode()
    else:
        content = rd.read().decode()

    print('_get_content: %s loaded ...%s bytes' % (url, len(content)))
    return content


def get_content(page, all_robots, timeout=60):
    print('get_content:', page)
    page_id, url, site_id, base_url = page
    content = _get_content(url, timeout)
    urls = all_robots.get(site_id).sitemaps
    # TODO: Выяснить тип контента
    print('get_content: %s **%s**' % (page, urls))
    return content, page, urls


def scan_page(page, all_robots):
    print('scan_page:', page, all_robots)
    page_id, url, site_id, base_url = page
    # request_time = time.time()
    logging.info('#BEGIN %s url %s, base_url %s', page_id, url, base_url)

    content = _get_content(url)

    all_robots = all_robots.get(site_id)

    return sitemap.scan_urls(content, page, all_robots)


def scan(max_limit=0, next_step=False):
    def get_content_complete(*result):
        print('get_content_complete:', result)

    def get_content_error(*result):
        print('get_content_error:', result)

    def process_ranks_complete(future):
        print('process_ranks_complete:', future.result())

    def scan_page_complete(future):
        def add_urls_complete(future):
            print('add_urls_complete:', future.result())

        """Страничная запись url'ов в БД"""

        new_pages_data, page_id, page_type = future.result()
        print('scan_page_complete:', page_id, page_type)

        # if page_type == sitemap.SM_TYPE_HTML:
        #     parsers.process_ranks(content, page_id)

        # [pool.apply_async(database.add_urls,
        #              (new_pages_data[r:r+settings.CHUNK_SIZE], page_id, page_type,))
        #              for r in range(0, len(new_pages_data), settings.CHUNK_SIZE)]

    database.add_robots()
    all_robots = robots.process_robots(database.get_robots())
    keywords = database.load_persons()

    # TODO: добавить проверку если len(pages) = 0 то найти наименьшую дату и выбрать по ней.
    # print('Crawler.scan: pages=%s' % len(pages))
    add_urls_total = 0

    pool = Pool(settings.POOL_SIZE)
    semaphore = Semaphore(settings.POOL_SIZE * 2)
    for page in database.get_pages_rows():
        # print('all_robots=', all_robots)
        with semaphore:
            print('scan:', page)
            pool.apply_async(get_content, (page, all_robots, semaphore),
                        callback=get_content_complete, callback)

    while poll._tasksqueue.qsize() > 0:
        time.sleep(1)
    pool.close()
    pool.join()
    logging.info('Crawler.scan: Add %s new urls on date %s', add_urls_total, 'NULL')
    return add_urls_total
