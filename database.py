import datetime
import logging
import settings
import MySQLdb

connection = None


def connect(conn_settings=settings.DB):
    global connection
    connection = MySQLdb.connect(**conn_settings)
    return connection


def close():
    connection.close()


def get_connect(conn_settings=None):
    conn_settings = conn_settings or settings.DB
    logging.debug("get_connect: connect to %s", conn_settings)
    return MySQLdb.connect(**conn_settings)


def load_persons(db=None):
    db = db or connection
    with db.cursor() as c:
        SELECT = 'select distinct Name, PersonID from keywords'
        c.execute(SELECT)
        logging.debug('load_persons: %s', c._last_executed)
        keywords = {}
        for n, i in c.fetchall():
            if not i in keywords.keys():
                keywords[i] = []
            keywords[i] += [n.lower(), ]

    logging.debug("load_persons: %s", keywords)
    return keywords


def get_robots(db=None):
    db = db or connection
    SELECT = ('SELECT p.ID, p.Url, p.SiteID, s.Name FROM pages p '
              'JOIN sites s ON (s.ID=p.SiteID) '
              'WHERE p.Url like "%/robots.txt"')
    with db.cursor() as c:
        c.execute(SELECT)
        for row in c.fetchall():
            yield row


def add_robots(db=None):
    """ Добавляет в pages ссылки на robots.txt, если их нет для определенных сайтов  """
    db = db or connection
    new_sites = _not_have_pages()
    ARGS = [{
            'site_id': r[1],
            'url': '%s/robots.txt' % r[0],
            'found_date_time': datetime.datetime.now(),
            'last_scan_date': None } for r in new_sites]
    _add_robots = add_urls(ARGS)

    logging.info('add_robots: %s robots url was add', _add_robots)
    return _add_robots


def _not_have_pages(db=None):
    """ Возвращает rows([site_name, site_id]) у которых нет страниц"""
    db = db or connect()
    with db.cursor() as c:
        c.execute('select s.Name, s.ID '
                    'from sites s '
                    'left join pages p on (p.SiteID=s.ID) '
                    'where p.id is Null')
        rows = c.fetchall()
    # c.close()
    return rows


def update_person_page_rank(page_id, ranks, found_datetime, db=None):
    db = db or connection
    if ranks:
        #logging.debug('update_person_page_rank: %s %s', page_id, ranks)
        SELECT = 'select id, rank from person_page_rank where PageID=%s and PersonID=%s and Scan_date_datetime=%s'
        UPDATE = 'update person_page_rank set Rank=%s where ID=%s'
        INSERT = 'insert into person_page_rank (PageID, PersonID, Rank, Scan_date_datetime) values (%s, %s, %s, %s)'
        found_datetime = datetime.datetime.now()
        for person_id, rank in ranks.items():
            if rank > 0:
                # Реализация INSERT OR UPDATE, т.к. кое кто отказался добавить UNIQUE_KEY :)

                with db.cursor() as c:
                    try:
                        c.execute(SELECT, (page_id, person_id, found_datetime))
                        rank_id, rank_ = c.fetchone()
                    except:
                        rank_id, rank_ = None, 0

                with db.cursor() as c:
                    if rank_id:
                        c.execute(UPDATE, (rank, rank_id, ))
                    else:
                        c.execute(INSERT, (page_id, person_id, rank, found_datetime))
                    db.commit()


def update_last_scan_date(page_id, db=connect()):
    # db = db or connection
    with db.cursor() as c:
        logging.debug('update_last_scan_date: update pages set LastScanDate=%s where ID=%s' % (datetime.datetime.now(), page_id))
        c.execute('update pages set LastScanDate=%s where ID=%s',
                  (datetime.datetime.now(), page_id))

        db.commit()
    db.close()


def get_pages_rows(last_scan_date=None, max_limit=0, db=None):
    db = db or connection
    SELECT = 'select p.id, p.Url, p.SiteID, s.Name, p.FoundDateTime '\
             'from pages p '\
             'join sites s on (s.ID=p.SiteID)'
    LIMIT = (' LIMIT %s' % max_limit) if max_limit > 0 else ''
    if last_scan_date is None:
        WHERE = 'where p.LastScanDate is null'
    else:
        WHERE = 'where p.LastScanDate = %s'

    query = ' '.join([SELECT, WHERE, LIMIT])

    with db.cursor() as c:
        c.execute(query, (last_scan_date))
        for page in c.fetchall():
            yield page


def add_urls(pages_data, db=None):
    """
        pages_data - dict(site_id, url, found_date_time, last_scan_date)
        добавляет url в таблицу pages если такой ссылки нет
    """
    db = db or connection
    logging.info('add_urls inserting %s', len(pages_data))
    if pages_data == []:
        return 0
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
    with db.cursor() as c:
        c.executemany(INSERT, pages_data)
        rows += c.rowcount
        db.commit()
    return rows


def add_urls_mp(pages_data, page_id=None, db=connect()):
    logging.info('add_urls_mp inserting %s', len(pages_data))
    rows = 0
    INSERT = ('INSERT INTO pages (SiteID, Url, FoundDateTime, LastScanDate, hash_url) '
              'VALUES (%(site_id)s, %(url)s, %(found_date_time)s, '
              '%(last_scan_date)s, MD5(%(url)s)) '
              'ON DUPLICATE KEY UPDATE FoundDateTime=%(found_date_time)s')

    logging.info('add_urls_mp inserting %s', db)
    with db.cursor() as c:
        try:
            c.executemany(INSERT, pages_data)
            rows += c.rowcount
            db.commit()
        except MySQLdb.Error as e:
            logging.info('add_urls_mp exception %s', e)
            raise Exception(e, pages_data, page_id)
        db.close()
    logging.info('add_urls_mp complete %s', len(pages_data))
    return rows, page_id
