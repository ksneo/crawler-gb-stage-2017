#!/usr/bin/python3
import datetime
from io import BytesIO
from multiprocessing import Pool
import urllib.request
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


def _get_content(url):
    # print('%s loading ...', url)
    try:
        rd = urllib.request.urlopen(url)
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


def get_content_mp(page, all_robots):
    page_id, url, site_id, base_url = page
    content = _get_content(url)
    return (content, page, all_robots)


def scan_page(page, all_robots):
    print('scan_page:', page, all_robots)
    page_id, url, site_id, base_url = page
    # request_time = time.time()
    logging.info('#BEGIN %s url %s, base_url %s', page_id, url, base_url)

    content = _get_content(url)

    all_robots = all_robots.get(site_id)

    return sitemap.scan_urls(content, page, all_robots)


def scan(next_step=False, max_limit=0):
    urls_limits = {}
    pool_size = [0, ]

    def get_content_complete(*args):
        content, page, all_robots = args[0]
        page_id, url, site_id, base_url = page
        if site_id not in urls_limits.keys():
            urls_limits[site_id] = 0
        urls_limits[site_id] += 1
        # print('get_content_complete: %s/%s' % (urls_limits[site_id], max_limit), page)
        pool_size[0] -= 1
        if (max_limit == 0) or (urls_limits[site_id] < max_limit):
            print('get_content_complete continue: %s/%s' % (urls_limits[site_id], max_limit), page)
            robots = all_robots.get(site_id)
            pool_size[0] += 1
            pool.apply_async(sitemap.scan_urls, (content, page, robots,),
                             callback=scan_page_complete, error_callback=scan_error)
            pool_size[0] += 1
            pool.apply_async(parsers.process_ranks, (content, page_id, keywords,),
                             callback=process_ranks_complete, error_callback=scan_error)

    def process_ranks_complete(ranks):
        pool_size[0] -= 1
        print('process_ranks_complete:', ranks)

    def scan_page_complete(*args):
        """Страничная запись url'ов в БД"""
        pool_size[0] -= 1
        print('scan_page_complete:', args[0])
        new_pages_data, page_id, page_type = args[0]

        # if page_type == sitemap.SM_TYPE_HTML:
        #     parsers.process_ranks(content, page_id)
        for r in range(0, len(new_pages_data), settings.CHUNK_SIZE):
            pool_size[0] += 1
            pool.apply_async(database.add_urls,
                     (new_pages_data[r:r+settings.CHUNK_SIZE], page_id, page_type,))

    def scan_error(error):
        pool_size[0] -= 1
        logging.error('scan_error: %s -- %s' % (pool_size[0], error))

    all_robots = robots.process_robots()
    keywords = database.load_persons()
    pool = Pool(settings.POOL_SIZE)

    # print(all_robots)
    # pages = database.get_pages_rows(None)
    # TODO: добавить проверку если len(pages) = 0 то найти наименьшую дату и выбрать по ней.
    # print('Crawler.scan: pages=%s' % len(pages))
    add_urls_total = 0
    # scans = []
    for page in database.get_pages_rows(max_limit=max_limit):
        pool_size[0] += 1
        pool.apply_async(get_content_mp, (page, all_robots),
                            callback=get_content_complete, error_callback=scan_error)
        while pool._taskqueue.qsize() > settings.WORK_LIMIT:
            print(pool._taskqueue.qsize())
            time.sleep(settings.WORK_TIMEOUT)

    while pool_size[0] > 0:
        # Ожидание опустошения пула
        time.sleep(1)
        print('pool.qsize:', pool_size, pool._taskqueue.qsize())
    pool.close()
    pool.join()
    logging.info('Crawler.scan: Add %s new urls on date %s', add_urls_total, 'NULL')
    return add_urls_total
