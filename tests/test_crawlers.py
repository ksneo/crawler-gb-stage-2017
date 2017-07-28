import pytest
from test_crawlers_fixtures import *
from test_parsers_fixtures import *

from crawlers import Crawler
from database import get_pages_rows, add_robots

def setup_module(module):
    clean_test_db()

def describe_crawlers_module():
    def describe_crawler_class():
        def it_create_obj():
            crawler = Crawler()
            assert isinstance(crawler, Crawler)
        
        #@pytest.mark.skip(reason="no way of currently testing this")
        def it_method_scan_urls_return_add_urls_count(test_db):
            add_robots()
            pages = get_pages_rows(None, test_db)
            crawler = Crawler()
            add_urls_count = crawler.scan_urls(pages, 300)
            assert add_urls_count == 1
        
        #@pytest.mark.skip(reason="no way of currently testing this")
        def it_method_scan_urls_return_add_urls_count_2(test_db):
            pages = get_pages_rows(None, test_db)
            crawler = Crawler()
            add_urls_count = crawler.scan_urls(pages, 1000)
            assert add_urls_count == 48
        
        @pytest.mark.skip(reason="very long operation 54s")
        def it_method_scan_urls_return_add_urls_count_3(test_db):
            pages = get_pages_rows(None, test_db)
            crawler = Crawler()
            add_urls_count = crawler.scan_urls(pages, 50000)
            assert add_urls_count == 51770

        def it_method_process_ranks_update_ranks(page_content):
            crawler = Crawler()
            result = crawler.process_ranks(page_content, 1)
            assert result == 0
