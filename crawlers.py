#!/usr/bin/python3
import datetime
import urllib.request
import time
import html.parser
import lxml
import settings


class Crawler:
    def __init__(self, next_step=False):
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
            # print(ARGS)
            c.executemany(INSERT, ARGS)
            self.db.commit()
        except Exception as e:
            self.db.rollback()
            print('Crawler exception', e)
        c.close()

        if next_step:
            scan_result = self.scan()

    def classify(self, url_data):
        return 'url_class'

    def scan(self):
        SELECT = 'select p.id, p.Url, p.SiteID '\
                 'from Pages p '\
                 'where p.LastScanDate is null'
        c = self.db.cursor()
        c.execute(SELECT)
        rows = 0
        while 1:
            row = c.fetchone()
            if not row: break
            rows += 1
            page_id, url, site_id = row
            url = url if url.startswith('http://') else 'http://' + url
            try:
                request_time = time.time()
                rd = urllib.request.urlopen(url)
                rd = rd.read().decode()
                url_class = self.classify(rd)
                request_time = time.time() - request_time
                print('Crawler.scan (%s): %s chars, %s sec' % (url, len(rd), request_time))
            except Exception as e:
                print('Crawler.scan (%s) exception %s' % (url, e))

        return rows

    def fresh(self):
        SELECT = ''

if __name__ == '__main__':
    c = Crawler()
    c.scan()
    c.fresh()

