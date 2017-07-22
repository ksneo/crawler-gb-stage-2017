import os
import urllib.request
from lxml import etree
import pytest
import sitemap as tm
from test_sitemap_fixtures import *

def describe_sitemap_module():
    def describe__esc_apm():
        def it_replace_apm():
            test_str = '&test and & but &amp; &&'
            assert tm._esc_amp(test_str) == '&amp;test and &amp; but &amp; &amp;&amp;'

    def describe__select_items():
        def it_return_list_of_urls(xml_sitemap_clean, urls_list):
            xml_tree = etree.fromstring(xml_sitemap_clean)
            xpath = 'url/loc'
            assert tm._select_items(xml_tree, xpath) == urls_list
    
    def describe__parse_xml():
        def it_return_list_of_urls(xml_sitemap, urls_list):
            assert tm._parse_xml(xml_sitemap) == urls_list
