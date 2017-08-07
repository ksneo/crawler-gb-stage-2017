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
    print('_get_content: %s loading ...' % url)
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


def scan(next_step=False, max_limit=0):
    urls_limits = {}

    def get_content_error(error):
        print('get_content_error:', error)

    def get_content_complete(*args):
        content, page, robots = args[0]
        page_id, url, site_id, base_url = page
        page_type = parsers.get_file_type(content)
        if site_id not in urls_limits.keys():
            urls_limits[site_id] = 0
        urls_limits[site_id] += 1
        # print('get_content_complete: %s/%s' % (urls_limits[site_id], max_limit), page)
        if (max_limit == 0) or (urls_limits[site_id] < max_limit):
            print('get_content_complete: %s/%s' % (urls_limits[site_id], max_limit), page)
            # robots = all_robots.get(site_id)
            with pool_sem:
                """Сканирование на наличие url'ов"""
                pool.apply_async(sitemap.scan_urls, (content, page, robots,),
                                 callback=scan_page_complete,
                                 error_callback=scan_page_error)
            if page_type == parsers.SM_TYPE_HTML:
                """Сканирование keywords"""
                with pool_sem:
                    pool.apply_async(parsers.process_ranks,
                                     (content, page_id, keywords,),
                                     callback=process_ranks_complete,
                                     error_callback=process_ranks_error)

    def process_ranks_complete(*args):
        ranks, page_id = args[0]
        print('process_ranks_complete:', ranks, page_id)
        database.update_person_page_rank(page_id, ranks)


    def process_ranks_error(error):
        print('process_ranks_error:', error)

    def scan_page_complete(*args):
        new_pages_data, page_id, page_type = args[0]
        print('scan_page_complete:', page_id, len(new_pages_data), page_type)
        with pool_sem:
            pool.apply_async(database.add_urls,
                             (new_pages_data,
                              page_id, page_type,),
                             callback=add_urls_complete,
                             error_callback=add_urls_error)

    def add_urls_complete(*args):
        rows, page_id = args[0]
        print('add_urls_complete:', args[0])
        database.update_last_scan_date(page_id)

    def add_urls_error(error):
        print('add_urls_error:', error)

    def scan_page_error(error):
        logging.error('scan_page_error: %s -- %s' % (pool_size[0], error))

    global pool
    pool = Pool(settings.POOL_SIZE)
    global pool_sem
    pool_sem = Semaphore(settings.POOL_SIZE * 2)

    database.add_robots()
    all_robots = robots.process_robots(database.get_robots())
    keywords = database.load_persons()

    # TODO: добавить проверку если len(pages) = 0 то найти наименьшую дату и выбрать по ней.
    # print('Crawler.scan: pages=%s' % len(pages))
    add_urls_total = 0

    for page in database.get_pages_rows(max_limit=max_limit):
        with pool_sem:
            # print('scan.pool:', pool_sem.get_value())
            pool.apply_async(get_content, (page, all_robots),
                             callback=get_content_complete,
                             error_callback=get_content_error)
            # print('scan.pool_size:', pool._taskqueue.qsize())

    while True:
        # Ожидание опустошения пула
        time.sleep(1)
        print('pool.qsize:', pool._taskqueue.qsize())
    pool.close()
    pool.join()
    logging.info('Crawler.scan: Add %s new urls on date %s', add_urls_total, 'NULL')
    return add_urls_total
