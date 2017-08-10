#!/usr/bin/python3
import datetime
from io import BytesIO
from multiprocessing import Pool, BoundedSemaphore, Semaphore
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
    logging.info('_get_content: %s loading ...' % url)
    try:
        rd = urllib.request.urlopen(url, timeout=timeout)
    except Exception as e:
        logging.error('_get_content (%s) exception %s' % (url, e))
        return ''
    charset = rd.headers.get_content_charset('utf-8') 
    logging.debug('_get_content: charset %s', charset) 
    content = ''
    try:
        if url.strip().endswith('.gz'):
            mem = BytesIO(rd.read())
            mem.seek(0)
            f = gzip.GzipFile(fileobj=mem, mode='rb')
            content = f.read().decode()
        else:
            content = rd.read().decode(charset)
    except UnicodeDecodeError as e:
        logging.error('_get_content: url = %s, charset = %s, error = %s', url, charset, e)

    logging.info('_get_content: %s loaded ...%s bytes' % (url, len(content)))
    return content


def _get_content_mp(page, all_robots, timeout=60):
    logging.info('get_content: %s' % (page,))
    page_id, url, site_id, base_url, found_datetime = page
    content = _get_content(url, timeout)
    urls = all_robots.get(site_id).sitemaps
    # TODO: Выяснить тип контента
    logging.info('get_content: %s %s' % ((page,), (urls,)))
    return content, page, urls


def _init_crawler():
    database.add_robots()
    all_robots = robots.process_robots(database.get_robots())
    keywords = database.load_persons()
    return keywords, all_robots


def scan(next_step=False, max_limit=0):
    if settings.MULTI_PROCESS:
        result = scan_mp(next_step, max_limit)
    else:
        result = scan_sp(next_step, max_limit)
    return result

def scan_sp(next_step=False, max_limit=0):
    keywords, all_robots = _init_crawler()
    pages = database.get_pages_rows(None)
    # TODO: добавить проверку если len(pages) = 0 то найти наименьшую дату и выбрать по ней.
    add_urls_total = 0
    # print('Crawler.scan: pages=%s' % len(pages))
    for page in pages:
        page_id, url, site_id, base_url, found_datetime = page
        request_time = time.time()
        logging.info('#BEGIN %s url %s, base_url %s', page_id, url, base_url)
        content = _get_content(url)
        robots = all_robots.get(site_id)

        if add_urls_total >= max_limit:
            page_type = sitemap.get_file_type(content)
            add_urls_count = 0
        else:
            new_pages_data, page_id, page_type = sitemap.scan_urls(content, page, robots)
            if len(new_pages_data) > max_limit:
                new_pages_data = new_pages_data[:max_limit + 1]
            add_urls_count = database.add_urls(new_pages_data)
            if page_type != sitemap.SM_TYPE_HTML:
                database.update_last_scan_date(page_id)

        if page_type == sitemap.SM_TYPE_HTML:
            ranks, page_id, found_datetime = parsers.process_ranks(content, page_id, keywords, found_datetime)
            database.update_person_page_rank(page_id, ranks, found_datetime)

        request_time = time.time() - request_time
        logging.info('#END url %s, base_url %s, add urls %s, time %s',
                        url, base_url, add_urls_count, request_time)
        add_urls_total = add_urls_total + add_urls_count

    logging.info('Crawler.scan: Add %s new urls on date %s', add_urls_total, 'NULL')
    return add_urls_total


def scan_mp(next_step=False, max_limit=0):
    urls_limits = {}

    def get_content_error(*error):
        print('get_content_error: %s' % error)

    def get_content_complete(*args):
        content, page, robots = args[0]
        page_id, url, site_id, base_url, found_datetime = page
        page_type = parsers.get_file_type(content)
        if site_id not in urls_limits.keys():
            urls_limits[site_id] = 0
        urls_limits[site_id] += 1
        # print('get_content_complete: %s/%s' % (urls_limits[site_id], max_limit), page)
        if (max_limit == 0) or (urls_limits[site_id] < max_limit):
            logging.info('get_content_complete: %s/%s %s' % (urls_limits[site_id], max_limit, page))
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
                                     (content, page_id, keywords, found_datetime),
                                     callback=process_ranks_complete,
                                     error_callback=process_ranks_error)

    def process_ranks_complete(*args):
        ranks, page_id, found_datetime = args[0]
        logging.info('process_ranks_complete: %s %s' % (ranks, page_id))
        database.update_person_page_rank(page_id, ranks, found_datetime)

    def process_ranks_error(*error):
        logging.error('process_ranks_error: %s' % error)

    def scan_page_complete(*args):
        new_pages_data, page_id, page_type = args[0]
        logging.info('scan_page_complete: %s %s %s' % (page_id, len(new_pages_data), page_type))
        for r in range(0, len(new_pages_data), settings.CHUNK_SIZE):
            with pool_sem:
                pool.apply_async(database.add_urls_mp,
                                 (new_pages_data[r:r+settings.CHUNK_SIZE],
                                  page_id,
                                  page_type==sitemap.SM_TYPE_HTML,),
                                 callback=add_urls_complete,
                                 error_callback=add_urls_error)

    def add_urls_complete(*args):
        rows, page_id = args[0]
        logging.info('add_urls_complete: %s %s' % (rows, page_id))
        database.update_last_scan_date(page_id)

    def add_urls_error(*error):
        logging.error('add_urls_error: %s' % error)
        # TODO: Поставить сбойнувший CHUNK в очередь

    def scan_page_error(*error):
        logging.error('scan_page_error: %s' % error)

    global pool
    pool = Pool(settings.POOL_SIZE)
    global pool_sem
    # pool_sem = BoundedSemaphore()
    pool_sem = Semaphore(settings.POOL_SIZE * 2)
    # pool._taskqueue.maxsize = settings.POOL_SIZE * 2

    keywords, all_robots = _init_crawler()

    # TODO: добавить проверку если len(pages) = 0 то найти наименьшую дату и выбрать по ней.
    # print('Crawler.scan: pages=%s' % len(pages))
    add_urls_total = 0

    for page in database.get_pages_rows(max_limit=max_limit):
        with pool_sem:
            add_urls_total += 1
            print('scan.pool:', pool_sem.get_value())
            pool.apply_async(_get_content_mp, (page, all_robots),
                             callback=get_content_complete,
                             error_callback=get_content_error)
            # print('scan.pool_size:', pool._taskqueue.qsize())

    close_pool_wait(add_urls_total)
    logging.info('Crawler.scan: Add %s new urls on date %s' % (add_urls_total, 'NULL'))
    return add_urls_total


def close_pool_wait(add_urls_total):
    # print(pool_sem.get_value())
    while True:
        # Ожидание опустошения пула
        time.sleep(1)
        print('pool.qsize:', pool_sem.get_value())
    pool.close()
    pool.join()
