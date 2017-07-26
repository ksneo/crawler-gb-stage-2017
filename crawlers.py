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
from robots import RobotsTxt

class Crawler:
    def __init__(self, db, next_step=False):
        """ п.1 в «Алгоритме ...» """
        self.db = db
        self._add_robots(db)

        self.keywords = self.load_persons()
        if next_step:
            print('Crawler: переходим к шагу 2 ...')
            scan_result = self.scan()

    def _not_have_pages(self, db):
        """ Возвращает rows([site_name, site_id]) у которых нет страниц"""
        c = db.cursor()
        c.execute('select s.Name, s.ID '
                  'from sites s '
                  'left join pages p on (p.SiteID=s.ID) '
                  'where p.id is Null')
        rows = c.fetchall()
        c.close()
        return rows

    def _add_robots(self, db):
        """ Добавляет в pages ссылки на robots.txt, если их нет для определенных сайтов """
        INSERT = 'insert into pages(SiteID, Url, LastScanDate, FoundDateTime) values (%s, %s, %s, %s)'
        new_sites = self._not_have_pages(db)
        ARGS = [(r[1], '%s/robots.txt' % r[0], None, datetime.datetime.now()) for r in new_sites]
        c = db.cursor()
        
        try:
            c.executemany(INSERT, ARGS)
            db.commit()
        except Exception as ex:
            db.rollback()
            logging.error('_add_robots: ARGS %s, exception %s', ARGS, ex)
        add_robots = c.rowcount
        c.close()
        logging.info('_add_robots: %s robots url was add', add_robots)
        return add_robots

    def load_persons(self):
        c = self.db.cursor()
        SELECT = 'select distinct Name, PersonID from keywords'
        c.execute(SELECT)
        keywords = {}
        for n, i in c.fetchall():
            if not i in keywords.keys():
                keywords[i] = []
            keywords[i] += [n, ]
        c.close()
        return keywords

    def update_last_scan_date(self, page_id):
        c = self.db.cursor()
        c.execute('update pages set LastScanDate=%s where ID=%s',
                  (datetime.datetime.now(), page_id))
        self.db.commit()
        c.close()

    def update_person_page_rank(self, page_id, ranks):
        if ranks:
            SELECT = 'select id from person_page_rank where PageID=%s and PersonID=%s'
            UPDATE = 'update person_page_rank set Rank=%s where ID=%s'
            INSERT = 'insert into person_page_rank (PageID, PersonID, Rank) values (%s, %s, %s)'
            for person_id, rank in ranks.items():
                c = self.db.cursor()
                c.execute(SELECT, (page_id, person_id))
                rank_id = c.fetchone()
                c.close()
                # Реализация INSERT OR UPDATE, т.к. кое кто отказался добавить UNIQUE_KEY :)
                c = self.db.cursor()
                if rank_id:
                    c.execute(UPDATE, (rank, rank_id))
                else:
                    c.execute(INSERT, (page_id, person_id, rank))
                self.db.commit()
                c.close()
    
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
    
    def _get_pages_rows(self, last_scan_date, db):
        SELECT = ('select p.id, p.Url, p.SiteID, s.Name '
                 'from pages p '
                 'join sites s on (s.ID=p.SiteID)')
        
        if last_scan_date is None:
            WHERE = 'where p.LastScanDate is null'
        else:
            WHERE = 'where p.LastScanDate = %s'

        query = ' '.join([SELECT, WHERE])
        
        with db.cursor() as c:
            c.execute(query, (last_scan_date))
            pages = c.fetchall()

        return pages

    def _is_robot_txt(self, url):
        return url.upper().endswith('ROBOTS.TXT')

    def _add_urls(self, pages_data, db):
        """
            pages_data - tuple(siteid, url, founddatatime, lastscandate, url)
            db - соединение с базой данных
            добавляет url в таблицу pages если такой ссылки нет
            решение взято отсюда 
            https://stackoverflow.com/questions/3164505/mysql-insert-record-if-not-exists-in-table
        """
        logging.info('Crawler._add_urls inserting %s', len(pages_data))

        INSERT = ('INSERT INTO pages (SiteID, Url, FoundDateTime, LastScanDate) ' 
                'SELECT * FROM (SELECT %s, %s, %s, %s) AS tmp '
                'WHERE NOT EXISTS (SELECT Url FROM pages WHERE Url = %s ) LIMIT 1')
        c = db.cursor()
        #c.executemany(INSERT, pages_data)
        rows = 0
        for page in pages_data:
            c.execute(INSERT, page)
            row = c.rowcount
            rows = rows + (row if row > 0 else 0)
            db.commit()
        
        c.close()
        return rows 

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
            urls_count = self._add_urls(pages_data, self.db)
            add_urls_count = add_urls_count + (urls_count if urls_count > 0 else 0)
            request_time = time.time() - request_time
            logging.info('#END url %s, base_url %s, add urls %s, time %s', 
                        url, base_url, urls_count, request_time)
            if max_limit > 0 and add_urls_count >= max_limit:
                break
        return add_urls_count

    def scan(self):
        pages = self._get_pages_rows(None, self.db)
        rows = self.scan_urls(pages)
        logging.info('Add %s new urls on date %s', rows, 'NULL')

    def fresh(self):
        SELECT = ''


if __name__ == '__main__':
    db = settings.DB
    c = Crawler(db)
    c.scan()
    c.fresh()
