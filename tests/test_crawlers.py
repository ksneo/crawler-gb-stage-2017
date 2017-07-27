import pytest
from test_crawlers_fixtures import *

from crawlers import Crawler

def setup_module(module):
    clean_test_db()

def describe_crawlers_module():
    def describe_crawler_class():
        def it_create_obj(test_db):
            crawler = Crawler(test_db)
            assert isinstance(crawler, Crawler)
        
        def it_method__get_pages_row_return_collection(test_db):
            crawler = Crawler(test_db)
            rows = crawler._get_pages_rows(None, test_db)
            assert len(rows) == 2
        
        def it_method_scan_urls_return_add_urls_count(test_db):
            crawler = Crawler(test_db)
            pages = crawler._get_pages_rows(None, test_db)
            add_urls_count = crawler.scan_urls(pages, 300)
            assert add_urls_count == 1
