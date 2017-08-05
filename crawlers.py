#!/usr/bin/python3
import datetime
from io import BytesIO
# from multiprocessing import Pool
import urllib.request
import asyncio
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
    # print('%s loading ...', url)
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

    print('%s loaded ...%s bytes' % (url, len(content)))
    return content


@asyncio.coroutine
def get_content_mp(page, all_robots, timeout=60):
    page_id, url, site_id, base_url = page
    content = _get_content(url, timeout)
    # print('get_content_mp:', content)
    # page_id, url, site_id, base_url = page
    all_robots = all_robots.get(site_id)
    # pool.apply_async(sitemap.scan_urls, (content, page, robots,), callback=scan_page_complete, error_callback=scan_error)

    # pool.apply_async(parsers.process_ranks, (content, page_id, keywords,), callback=process_ranks_complete, error_callback=scan_error)
    return content, page, all_robots


def scan_page(page, all_robots):
    print('scan_page:', page, all_robots)
    page_id, url, site_id, base_url = page
    # request_time = time.time()
    logging.info('#BEGIN %s url %s, base_url %s', page_id, url, base_url)

    content = _get_content(url)

    all_robots = all_robots.get(site_id)

    return sitemap.scan_urls(content, page, all_robots)


@asyncio.coroutine
def scan(loop, max_limit=0, next_step=False):
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

        fut = asyncio.Future()
        asyncio.ensure_future(database.add_urls(fut, new_pages_data, page_id, page_type))
        fut.add_done_callback(add_urls_complete)
        # [pool.apply_async(database.add_urls,
        #              (new_pages_data[r:r+settings.CHUNK_SIZE], page_id, page_type,))
        #              for r in range(0, len(new_pages_data), settings.CHUNK_SIZE)]

    all_robots = robots.process_robots()
    print('all_robots=', all_robots)
    keywords = database.load_persons()
    print('keywords=', keywords)

    # TODO: добавить проверку если len(pages) = 0 то найти наименьшую дату и выбрать по ней.
    # print('Crawler.scan: pages=%s' % len(pages))
    add_urls_total = 0

    futures = set()
    for page in database.get_pages_rows():
        print(page)
        if len(futures) < settings.POOL_SIZE:
            futures.add(get_content_mp(page, all_robots, timeout=60))
        else:
            for future in asyncio.as_completed(futures):
                content, page, robots_ = yield from future
                # pool.apply_async(sitemap.scan_urls, (content, page, robots,), callback=scan_page_complete, error_callback=scan_error)
                fut = asyncio.Future()
                asyncio.ensure_future(sitemap.scan_urls(fut, content, page, robots_,))
                page_id = page[1]
                fut.add_done_callback(scan_page_complete)
                # pool.apply_async(parsers.process_ranks, (content, page_id, keywords,), callback=process_ranks_complete, error_callback=scan_error)
                fut = asyncio.Future()
                asyncio.ensure_future(parsers.process_ranks(fut, content, page_id, keywords,))
                fut.add_done_callback(process_ranks_complete)
            futures = set()

    logging.info('Crawler.scan: Add %s new urls on date %s', add_urls_total, 'NULL')
    return add_urls_total
