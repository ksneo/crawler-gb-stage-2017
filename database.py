import datetime
import logging
import settings
import MySQLdb


def load_persons(db=settings.DB):
    # db = settings.DB
    with db.cursor() as c:
        SELECT = 'select distinct Name, PersonID from keywords'
        c.execute(SELECT)
        logging.debug('load_persons: %s', c._last_executed)
        keywords = {}
        for n, i in c.fetchall():
            if not i in keywords.keys():
                keywords[i] = []
            keywords[i] += [n.lower(), ]
    # c.close()
    logging.debug("load_persons: %s", keywords)
    return keywords


def get_robots(db=settings.DB):
    SELECT = ('SELECT p.ID, p.Url, p.SiteID, s.Name FROM pages p '
              'JOIN sites s ON (s.ID=p.SiteID) '
              'WHERE p.Url like "%/robots.txt"')
    with db.cursor() as c:
        c.execute(SELECT)
        for row in c.fetchall():
            yield row
    # logging.info('get_robots: %s', rows)
    # c.close()
    # return rows


def add_robots():
    """ Добавляет в pages ссылки на robots.txt, если их нет для определенных сайтов  """
    # db = settings.DB
    # INSERT = 'insert into pages(SiteID, Url, FoundDateTime, LastScanDate) values (%s, %s, %s, %s)'
    new_sites = _not_have_pages()
    # ARGS = [(r[1], '%s/robots.txt' % r[0], None, datetime.datetime.now(), hashlib.md5(('%s/robots.txt' % r[0]).encode()).hexdigest()) for r in new_sites]
    print('add_robots:', new_sites)
    ARGS = [{
            'site_id': r[1],
            'url': '%s/robots.txt' % r[0],
            'found_date_time': datetime.datetime.now(),
            'last_scan_date': None } for r in new_sites]
    _add_robots = add_urls(ARGS)

    logging.info('add_robots: %s robots url was add', _add_robots)
    return _add_robots


def _not_have_pages(db=settings.DB):
    """ Возвращает rows([site_name, site_id]) у которых нет страниц"""
    with db.cursor() as c:
        c.execute('select s.Name, s.ID '
                    'from sites s '
                    'left join pages p on (p.SiteID=s.ID) '
                    'where p.id is Null')
        rows = c.fetchall()
    # c.close()
    return rows


def update_person_page_rank(page_id, ranks, db=settings.DB):
    if ranks:
        logging.debug('update_person_page_rank: %s %s', page_id, ranks)
        SELECT = 'select id from person_page_rank where PageID=%s and PersonID=%s'
        UPDATE = 'update person_page_rank set Rank=%s where ID=%s'
        INSERT = 'insert into person_page_rank (PageID, PersonID, Rank) values (%s, %s, %s)'
        for person_id, rank in ranks.items():
            if rank > 0:
                # Реализация INSERT OR UPDATE, т.к. кое кто отказался добавить UNIQUE_KEY :)

                with db.cursor() as c:
                    c.execute(SELECT, (page_id, person_id))
                    rank_id = c.fetchone()

                with db.cursor() as c:
                    if rank_id:
                        c.execute(UPDATE, (rank, rank_id))
                    else:
                        c.execute(INSERT, (page_id, person_id, rank))
                    db.commit()


def update_last_scan_date(page_id, db=settings.DB):
    with db.cursor() as c:
        c.execute('update pages set LastScanDate=%s where ID=%s',
                (datetime.datetime.now(), page_id))
        logging.debug('update_last_scan_date: %s', c._last_executed)

        db.commit()


def get_pages_rows(last_scan_date=None, max_limit=0, db=settings.DB):
    # db = settings.DB
    SELECT = 'select p.id, p.Url, p.SiteID, s.Name '\
             'from pages p '\
             'join sites s on (s.ID=p.SiteID)'
    LIMIT = ' LIMIT %s' % max_limit if max_limit > 0 else ''
    if last_scan_date is None:
        WHERE = 'where p.LastScanDate is null'
    else:
        WHERE = 'where p.LastScanDate = %s'

    query = ' '.join([SELECT, WHERE, LIMIT])

    with db.cursor() as c:
        c.execute(query, (last_scan_date))
        for page in c.fetchall():
            yield page

    # return pages


def _add_urls(pages_data, page_id=None, page_type_html=False, db=settings.DB):
    logging.info('add_urls inserting %s' % len(pages_data))
    # print('_add_urls:', pages_data)

    # медленный вариант, но работает без добавления дополнительного поля
    # отчасти был медленным из-за настройки mysql сервера, но и так разница в 3 раза
    # INSERT = ('INSERT INTO pages (SiteID, Url, FoundDateTime, LastScanDate) '
    #         'SELECT * FROM (SELECT %s, %s, %s, %s) AS tmp '
    #         'WHERE NOT EXISTS (SELECT Url FROM pages WHERE Url = %s ) LIMIT 1')

    INSERT = ('INSERT INTO pages (SiteID, Url, FoundDateTime, LastScanDate, hash_url) '
              'VALUES (%(site_id)s, %(url)s, %(found_date_time)s, '
              '%(last_scan_date)s, MD5(%(url)s)) '
              'ON DUPLICATE KEY UPDATE FoundDateTime=%(found_date_time)s')

    rows = 0
    for page in pages_data:
        with db.cursor() as c:
            c.execute(INSERT, page)
            rows += c.rowcount
    db.commit()

    # if page_type_html and page_id:
    #     update_last_scan_date(page_id)
    # print('_add_urls %s completed...' % rows)
    return rows, page_id


def add_urls(pages_data, page_id=None, page_type_html=False, db=settings.DB):
    """
        pages_data - dict(site_id, url, found_date_time, last_scan_date)
        добавляет url в таблицу pages если такой ссылки нет
    """
    return _add_urls(pages_data, page_id, page_type_html, db)
