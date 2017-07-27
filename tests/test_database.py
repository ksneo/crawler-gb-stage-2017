import pytest
from test_crawlers_fixtures import *
from database import _get_pages_rows

def setup_module(module):
    clean_test_db()

def describe_database_module():
    def describe__get_pages_rows():
        def it_method__get_pages_row_return_collection(test_db):
            rows = _get_pages_rows(None, test_db)
            assert len(rows) == 2
        
        def it_method_scan_urls_return_add_urls_count(test_db):
            pages = _get_pages_rows(None, test_db)
            add_urls_count = crawler.scan_urls(pages, 300)
            assert add_urls_count == 1

        def it_method_scan_urls_return_add_urls_count_2(test_db):
            crawler = Crawler(test_db)
            pages = crawler._get_pages_rows(None, test_db)
            add_urls_count = crawler.scan_urls(pages, 1000)
            assert add_urls_count == 48

        def it_method_scan_urls_return_add_urls_count_3(test_db):
            crawler = Crawler(test_db)
            pages = crawler._get_pages_rows(None, test_db)
            add_urls_count = crawler.scan_urls(pages, 50000)
            assert add_urls_count == 48