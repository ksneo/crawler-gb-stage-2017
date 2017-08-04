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

    print('%s loaded ...%s bytes' % (url, len(content)))
    return content


def scan_page(page, all_robots):
    print('scan_page:', page, all_robots)
    page_id, url, site_id, base_url = page
    # request_time = time.time()
    logging.info('#BEGIN %s url %s, base_url %s', page_id, url, base_url)

    content = _get_content(url)

    all_robots = all_robots.get(site_id)

    return sitemap.scan_urls(content, page, all_robots)


def scan(next_step=False, max_limit=0):
    def scan_page_complete(*args):
        """Страничная запись url'ов в БД"""

        args = args[0]
        new_pages_data = args[0]
        page_id = args[1]
        page_type = args[2]

        # if page_type == sitemap.SM_TYPE_HTML:
        #     parsers.process_ranks(content, page_id)

        [pool.apply_async(database.add_urls,
                     (new_pages_data[r:r+settings.CHUNK_SIZE], page_id, page_type,))
                     for r in range(0, len(new_pages_data), settings.CHUNK_SIZE)]

    def scan_page_error(error):
        print('scan_page_error:', error)

    all_robots = robots.process_robots()
    pool = Pool(settings.POOL_SIZE)

    # print(all_robots)
    # pages = database.get_pages_rows(None)
    # TODO: добавить проверку если len(pages) = 0 то найти наименьшую дату и выбрать по ней.
    # print('Crawler.scan: pages=%s' % len(pages))
    add_urls_total = 0
    scans = []
    for page in database.get_pages_rows():

        scans.append(pool.apply_async(scan_page, (page, all_robots),
                            callback=scan_page_complete, error_callback=scan_page_error))
        # add_urls_total += self.scan_page(page, all_robots)
        # if add_urls_total >= self.max_limit:
        #     break
    # print('scan scans:', scans)
    while pool._taskqueue.qsize() > 0:
        # Ожидание опустошения пула
        time.sleep(1)
    pool.close()
    pool.join()
    logging.info('Crawler.scan: Add %s new urls on date %s', add_urls_total, 'NULL')
    return add_urls_total
