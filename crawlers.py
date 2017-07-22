#!/usr/bin/python3
import datetime
import urllib.request
import time
import html.parser
import lxml
import settings


class Crawler:
    def __init__(self, next_step=False):
        """ п.1 в «Алгоритме ...» """
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

    def update_last_scan_date(self, page_id):
        c = self.db.cursor()
        c.execute('update Pages set LastScanDate=? where id=?', (datetime.datetime.now(), page_id))
        self.db.commit()
        c.close()

    def scan(self):
        SELECT = 'select p.id, p.Url, p.SiteID '\
                 'from Pages p '\
                 'where p.LastScanDate is null'
        c = self.db.cursor()
        c.execute(SELECT)
        rows = 0
        for row in c.fetchall():
            rows += 1
            page_id, url, site_id = row
            url = url if url.startswith('http') else 'http://' + url
            try:
                request_time = time.time()
                rd = urllib.request.urlopen(url)
            except Exception as e:
                print('Crawler.scan (%s) exception %s' % (url, e))
            else:
                rd = rd.read().decode()
                # url_class = self.classify(rd)
                if url.upper().endswith('ROBOTS.TXT'):
                    sitemaps = {r.split(':')[1] for r in robots.split('\n')
                                if trim(r).upper().startswith('SITEMAP:')}
                    # TODO: Скинуть в базу
                request_time = time.time() - request_time
                print('Crawler.scan (%s): %s chars, in %s sec' % (url, rd, request_time))
                self.update_last_scan_date(page_id)

        c.close()
        return rows

    def fresh(self):
        SELECT = ''

if __name__ == '__main__':
    c = Crawler()
    c.scan()
    c.fresh()

