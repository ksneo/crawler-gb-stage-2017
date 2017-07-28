import pytest
from test_crawlers_fixtures import *
from database import get_pages_rows, add_robots

def setup_module(module):
    clean_test_db()

def describe_database_module():
    def describe__get_pages_rows():
        def it_method__get_pages_row_return_collection(test_db):
            add_robots()
            rows = get_pages_rows(None, test_db)
            assert len(rows) == 2
