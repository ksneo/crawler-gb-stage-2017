import pytest
import log
import settings
from test_crawlers_fixtures import *
from test_parsers_fixtures import *
import crawler
from robots import RobotsTxt
from database import get_pages_rows, add_robots, connect

def setup_module(module):
    clean_test_db()

def describe_crawlers_module():
    def describe_crawler():
        #@pytest.mark.skip(reason="no way of currently testing this")
        def it_method_init_crawler():
            database.connect(settings.TEST_DATABASE)
            _, robots = crawler._init_crawler()
            database.close()
            assert len(robots) == 2
            assert isinstance(robots[1], RobotsTxt)

        #@pytest.mark.skip(reason="no way of currently testing this")
        def it_method_scan_urls_return_add_urls_count():
            settings.MULTI_PROCESS = False
            database.connect(settings.TEST_DATABASE)
            result = crawler.scan(max_limit=300)
            database.close()
            assert result > 0

        #@pytest.mark.skip(reason="no way of currently testing this")
        def it_method_scan_urls_return_add_urls_count1():
            settings.MULTI_PROCESS = False
            database.connect(settings.TEST_DATABASE)
            result = crawler.scan(max_limit=300)
            database.close()
            assert result > 0

        @pytest.mark.skip(reason="very long operation 54s")
        def it_method_scan_urls_return_add_urls_count_50000():
            settings.MULTI_PROCESS = False
            database.connect(settings.TEST_DATABASE)
            result = crawler.scan(max_limit=50000)
            database.close()
            assert result > 50000

        @pytest.mark.skip(reason="very long operation 54s")
        def it_method_scan_urls_return_add_urls_count_html():
            result = crawler.scan(max_limit=50000)
            assert result == 51770

