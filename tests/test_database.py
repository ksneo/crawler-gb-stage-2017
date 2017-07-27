import pytest
from test_crawlers_fixtures import *
from database import get_pages_rows

def setup_module(module):
    clean_test_db()

@pytest.mark.skip(reason="no way of currently testing this")
def describe_database_module():
    def describe__get_pages_rows():
        def it_method__get_pages_row_return_collection(test_db):
            rows = get_pages_rows(None, test_db)
            assert len(rows) == 2
