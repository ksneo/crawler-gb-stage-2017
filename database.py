import datetime
import logging
import settings


def load_persons(db=settings.DB):
    # db = settings.DB
    c = db.cursor()
    SELECT = 'select distinct Name, PersonID from keywords'
    c.execute(SELECT)
    keywords = {}
    for n, i in c.fetchall():
        if not i in keywords.keys():
            keywords[i] = []
        keywords[i] += [n, ]
    c.close()
    return keywords


def add_robots(db=settings.DB):
    """ Добавляет в pages ссылки на robots.txt, если их нет для определенных сайтов """
    # db = settings.DB
    # INSERT = 'insert into pages(SiteID, Url, FoundDateTime, LastScanDate) values (%s, %s, %s, %s)'
    new_sites = _not_have_pages()
    # ARGS = [(r[1], '%s/robots.txt' % r[0], None, datetime.datetime.now(), hashlib.md5(('%s/robots.txt' % r[0]).encode()).hexdigest()) for r in new_sites]
    ARGS = [{
            'site_id': r[1],
            'url': '%s/robots.txt' % r[0],
            'found_date_time': datetime.datetime.now(),
            'last_scan_date': None } for r in new_sites]
    add_robots = add_urls(ARGS)

    logging.info('add_robots: %s robots url was add', add_robots)
    return add_robots


def _not_have_pages(db=settings.DB):
    """ Возвращает rows([site_name, site_id]) у которых нет страниц"""
    # db = settings.DB
    c = db.cursor()
    c.execute('select s.Name, s.ID '
                'from sites s '
                'left join pages p on (p.SiteID=s.ID) '
                'where p.id is Null')
    rows = c.fetchall()
    c.close()
    return rows


def update_person_page_rank(page_id, ranks, db=settings.DB):
    if ranks:
        # db = settings.DB
        SELECT = 'select id from person_page_rank where PageID=%s and PersonID=%s'
        UPDATE = 'update person_page_rank set Rank=%s where ID=%s'
        INSERT = 'insert into person_page_rank (PageID, PersonID, Rank) values (%s, %s, %s)'
        for person_id, rank in ranks.items():
            # Реализация INSERT OR UPDATE, т.к. кое кто отказался добавить UNIQUE_KEY :)
            c = db.cursor()
            c.execute(SELECT, (page_id, person_id))
            rank_id = c.fetchone()
            c.close()
            c = db.cursor()
            if rank_id:
                c.execute(UPDATE, (rank, rank_id))
            else:
                c.execute(INSERT, (page_id, person_id, rank))
            db.commit()
            c.close()


def update_last_scan_date(page_id, db=settings.DB):
    print('update_last_scan_date %s' % page_id)
    # db = settings.DB
    c = db.cursor()
    c.execute('update pages set LastScanDate=%s where ID=%s',
                (datetime.datetime.now(), page_id))
    db.commit()
    c.close()
    print('update_last_scan_date %s complete...' % page_id)


def get_pages_rows(last_scan_date, db=settings.DB):
    # db = settings.DB
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


def add_urls(pages_data, db=settings.DB):
    """
        pages_data - dict(site_id, url, found_date_time, last_scan_date)
        добавляет url в таблицу pages если такой ссылки нет
    """
    logging.info('add_urls inserting %s', len(pages_data))

    # медленный вариант, но работает без добавления дополнительного поля
    # отчасти был медленным из-за настройки mysql сервера, но и так разница в 3 раза
    # INSERT = ('INSERT INTO pages (SiteID, Url, FoundDateTime, LastScanDate) '
    #         'SELECT * FROM (SELECT %s, %s, %s, %s) AS tmp '
    #         'WHERE NOT EXISTS (SELECT Url FROM pages WHERE Url = %s ) LIMIT 1')

    INSERT = ('INSERT INTO pages (SiteID, Url, FoundDateTime, LastScanDate, hash_url) '
              'VALUES (%(site_id)s, %(url)s, %(found_date_time)s, %(last_scan_date)s, MD5(%(url)s))')

    c = db.cursor()
    rows = 0

    for page in pages_data:
        try:
            c.execute(INSERT, page)
            row = c.rowcount
            rows = rows + (row if row > 0 else 0)
            db.commit()
            # print('_add_urls', (page, ))
            print('+', end='', flush=True)
        except Exception as e:
            # logging.error('add_urls exception %s', e)
            # print('add_urls exception %s', e)
            print('.', end='', flush=True)
            db.rollback()

    c.close()
    print('\n_add_urls %s completed...' % rows)
    return rows

