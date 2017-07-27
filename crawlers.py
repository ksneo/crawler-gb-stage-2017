#!/usr/bin/python3
import datetime
from io import BytesIO
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
from robots import RobotsTxt


class Crawler:
    def __init__(self, next_step=False):
        """ п.1 в «Алгоритме ...» """
        # self.db = db
        database._add_robots()

        self.keywords = database.load_persons()
        if next_step:
            print('Crawler: переходим к шагу 2 ...')
            scan_result = self.scan()

    def _get_content(self, url):
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

        return content

    def _is_robot_txt(self, url):
        return url.upper().endswith('ROBOTS.TXT')

    def scan_urls(self, pages, max_limit=0):
        add_urls_count = 0
        for row in pages:
            _, url, site_id, base_url = row
            request_time = time.time()
            logging.info('#BEGIN url %s, base_url %s', url, base_url)
            urls = []
            sitemaps = []
            content = ""

            if self._is_robot_txt(url):
                robots_file = RobotsTxt(url)
                robots_file.read()
                sitemaps = robots_file.sitemaps
                #logging.info('find_maps: %s', sitemaps)
            else:
                content = self._get_content(url)
                urls, sitemaps = sitemap.get_urls(content, base_url)

            urls += sitemaps
            pages_data = [(site_id, u, datetime.datetime.now(), None, u) for u in urls if url]
            urls_count = database._add_urls(pages_data)
            add_urls_count = add_urls_count + (urls_count if urls_count > 0 else 0)
            request_time = time.time() - request_time
            logging.info('#END url %s, base_url %s, add urls %s, time %s',
                        url, base_url, urls_count, request_time)
            if max_limit > 0 and add_urls_count >= max_limit:
                break
        return add_urls_count

    def scan(self):
        pages = database._get_pages_rows(None)
        rows = self.scan_urls(pages)
        logging.info('Add %s new urls on date %s', rows, 'NULL')

    def fresh(self):
        SELECT = ''


if __name__ == '__main__':
    db = settings.DB
    c = Crawler(db)
    c.scan()
    c.fresh()
