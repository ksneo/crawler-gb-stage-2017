import os
import urllib.request
from lxml import etree
import pytest
import sitemap as tm

url = 'file:' + urllib.request.pathname2url(os.path.abspath('./tests/test_sitemap.xml'))

def describe_sitemap_module():
    def describe_load_sitemap():
        def it_return_xml_tree_with_content():
            awaited_xml_tree_xmlns = "urlset"
            assert tm.load_sitemap(url).tag == awaited_xml_tree_xmlns
    
    def describe_get_urls():
        def it_return_list_of_urls():
            awaited_list = ['https://yandex.ru/blog/yandexbrowser',
                            'https://yandex.ru/blog/yandexbrowser?year=2013&month=Dec',
                            'https://yandex.ru/blog/yandexbrowser?year=2013&month=Nov',
                            'https://yandex.ru/blog/yandexbrowser?year=2013&month=Oct',
                            'https://yandex.ru/blog/yandexbrowser?year=2013&month=Sep']
            xml_tree = tm.load_sitemap(url)
            xpath = 'url/loc'
            assert tm.get_urls(xml_tree, xpath) == awaited_list
