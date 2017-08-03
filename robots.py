from urllib.robotparser import RobotFileParser
from urllib.parse import unquote
import time
import logging
import database
import sitemap


class RobotsTxt(RobotFileParser):
    def __init__(self, url=''):
        self.__sitemaps = set()
        super().__init__(url)

    @property
    def sitemaps(self):
        """ sitemap type set """
        return self.__sitemaps

    def parse(self, lines):
        sitemaps = set()
        for line in lines:
            line = line.split(':', 1)
            if len(line) == 2:
                line[0] = line[0].strip().lower()
                line[1] = unquote(line[1].strip())
                if line[0] == 'sitemap':
                    sitemaps.add(line[1])
        self.__sitemaps = list(sitemaps)
        super().parse(lines)


def process_robots():
    """
        Производит обработку файлов robots.txt
        - добавляет в базу новые файлы robots
        - создает объекты из файлов robots.txt,
            которые умеют проверять ссылки и содежрат sitemaps
        - возвращает словарь site_id : RobotsTxt
    """
    result = {}
    database.add_robots()
    # robots_rows = database.get_robots()
    for robot in database.get_robots():
        page_id, url, site_id, base_url = robot
        request_time = time.time()
        logging.info('#BEGIN %s url %s, base_url %s', page_id, url, base_url)
        robots_file = RobotsTxt(url)
        robots_file.read()
        result[site_id] = robots_file
        urls = robots_file.sitemaps
        urls_count = sitemap.add_urls(urls, robot, sitemap.SM_TYPE_TXT)
        request_time = time.time() - request_time
        logging.info('#END url %s, base_url %s, add urls %s, time %s',
                        url, base_url, urls_count, request_time)
    return result

def _is_robot_txt(url):
    return url.upper().endswith('ROBOTS.TXT')

