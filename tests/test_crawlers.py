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