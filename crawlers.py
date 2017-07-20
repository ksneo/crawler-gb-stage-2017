#!/usr/bin/python3
import datetime
import time
import settings


class Crawler:
    def __init__(self):
        db = settings.DB
        c = db.cursor()
        c.execute('select s.name, s.id '
                  'from Sites s '
                  'left join Pages p on (p.SiteID=s.ID) '
                  'where p.id is Null')
        INSERT = 'insert into Pages(SiteID, Url) values (?, ?)'
        ARGS = [(r[1], '%s/robots.txt' % r[0]) for r in c.fetchall()]
        c.close()
        c = db.cursor()
        try:
            print(ARGS)
            c.executemany(INSERT, ARGS)
            db.commit()
        except Exception as e:
            db.rollback()
            print('Crawler exception', e)

        print('Crawler init done ...')


if __name__ == '__main__':
    c = Crawler()
