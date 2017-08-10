import pytest
import log
from test_crawlers_fixtures import *
from test_parsers_fixtures import *
import settings
import crawler
from robots import RobotsTxt
from database import get_pages_rows, add_robots

def setup_module(module):
    clean_test_db()

def describe_crawlers_module():
    def describe_crawler():
        #@pytest.mark.skip(reason="no way of currently testing this")
        def it_method_init_crawler(test_db):
            _, robots = crawler._init_crawler()
            assert len(robots) == 2
            assert isinstance(robots[1], RobotsTxt)

        #@pytest.mark.skip(reason="no way of currently testing this")
        def it_method_scan_urls_return_add_urls_count(test_db):
            settings.MULTI_PROCESS = False
            result = crawler.scan(max_limit=300)
            assert result > 0

        @pytest.mark.skip(reason="very long operation 54s")
        def it_method_scan_urls_return_add_urls_count_50000():
            result = crawler.scan(max_limit=50000)
            assert result > 50000

        @pytest.mark.skip(reason="very long operation 54s")
        def it_method_scan_urls_return_add_urls_count_html():
            result = crawler.scan(max_limit=50000)
            assert result == 51770

