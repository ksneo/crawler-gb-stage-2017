#!/usr/bin/python3
import datetime
from io import BytesIO
import urllib.request
import re
import gzip
import time
import html.parser
import lxml
import settings
import sitemap


class Crawler:
    def __init__(self, next_step=False):
        """ п.1 в «Алгоритме ...» """
        print('Crawler.__init__ ...')
        self.db = settings.DB
        c = self.db.cursor()
        c.execute('select s.name, s.id '
                  'from Sites s '
                  'left join Pages p on (p.SiteID=s.ID) '
                  'where p.id is Null')
        INSERT = 'insert into Pages(SiteID, Url) values (?, ?)'
        ARGS = [(r[1], '%s/robots.txt' % r[0]) for r in c.fetchall()]
        c.close()
        c = self.db.cursor()
        try:
            print(ARGS)
            c.executemany(INSERT, ARGS)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print('Crawler exception', e)
        c.close()

        if next_step:
            print('Crawler: переходим к шагу 2 ...')
            scan_result = self.scan()

    def classify(self, url_data):
        return 'url_class'

    def update_last_scan_date(self, page_id):
        c = self.db.cursor()
        c.execute('update Pages set LastScanDate=? where id=?',
                  (datetime.datetime.now(), page_id))
        self.db.commit()
        c.close()

    def scan(self):
        SELECT = 'select distinct p.id, p.Url, p.SiteID, s.name '\
                 'from Pages p '\
                 'join Sites s on (s.ID=p.SiteID) '\
                 'where p.LastScanDate is null'
        c = self.db.cursor()
        c.execute(SELECT)
        pages = c.fetchall()
        c.close()
        rows = 0
        for row in pages:
            rows += 1
            page_id, url, site_id, base_url = row
            url = ('http://' + url) if not (url.startswith('http://') or url.startswith('https://')) else url
            # if url.startswith('http://')
            # elif url.startswith('https://') else ('https://' + url)

            urls = []
            request_time = time.time()
            try:
                # print('Загрузка', url)
                rd = urllib.request.urlopen(url)
            except Exception as e:
                print('Crawler.scan (%s) exception %s' % (url, e))
            else:
                try:
                    if not url.strip().endswith('.gz'):
                        rd = rd.read()
                    else:
                        mem = BytesIO(rd.read())
                        mem.seek(0)
                        f = gzip.GzipFile(fileobj=mem, mode='rb')
                        rd = f.read()
                except Exception as e:
                    print('Crowlrer.read exception', e, url)
                else:
                    # if url.endswith('.xml.gz'):
                    #     print(url, rd)
                    # url_class = self.classify(rd)
                    if url.upper().endswith('ROBOTS.TXT'):
                        # print('Crawler: обработка %s ...' % url)
                        urls, sitemaps = list({r for r in re.findall('http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+', rd)}), []

                        # urls, sitemaps = [(site_id, r.split(':')[1],) for r in rd.split('\n')
                        #             if r.strip().upper().startswith('SITEMAP:')], []
                        # print('Crawler: %s.sitemap: %s' % (url, urls))
                    else:
                        try:
                            urls, sitemaps = sitemap.get_urls(rd.decode(), base_url)
                        except Exception as e:
                            print(base_url, rd[:20], ' ... ', rd[-20:], e)
                            # urls, sitemaps = [], []
                    urls += sitemaps
                    urls = [(site_id, u) for u in urls if url]
                    # print('Crawler: urls %s' % urls)
                    INSERT = 'insert into Pages (SiteID, url) values (?, ?)'
                    c = self.db.cursor()
                    c.executemany(INSERT, urls)
                    self.db.commit()
                    c.close()
                    # print('Crawler.scan (%s): %s, in %s sec, urls: %s' %
                            # (base_url, urls, request_time, urls))
                    self.update_last_scan_date(page_id)
            request_time = time.time() - request_time
        return rows

    def fresh(self):
        SELECT = ''


if __name__ == '__main__':
    c = Crawler()
    c.scan()
    c.fresh()
