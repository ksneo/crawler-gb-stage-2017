from urllib.parse import urlparse
from lxml import etree
import io
import re

def _esc_amp(text):
    """ text строка, возвращает строку с замененными & """ 
    # замена & на &amp;
    return re.sub(r'&(?!amp;)', r'&amp;', text)

def _get_nsless_xml(xml):
    """ xml - bytes[], возращает root ETreeElement """
    # убираем namespaces из xml
    it = etree.iterparse(xml, recover=True)
    for _, el in it:
        if '}' in el.tag:
            el.tag = el.tag.split('}', 1)[1]  # strip all namespaces
        for at in el.attrib.keys(): # strip namespaces of attributes too
            if '}' in at:
                newat = at.split('}', 1)[1]
                el.attrib[newat] = el.attrib[at]
                del el.attrib[at]
    return it.root

def _select_items(xml_elem, xpath):
    """ xml_elem ETreeElement, xpath - путь поиска, возвращает список урлов в элементе """
    items = [x.text.strip() for x in xml_elem.xpath(xpath)]
    return items

def _select_attrs(xml_elem, xpath):
    """ xml_elem ETreeElement, xpath - путь поиска, с выбором атрибутов """
    attrs = [x.strip() for x in xml_elem.xpath(xpath)]
    return attrs

def _parse_txt(content):
    """
        content - содержимое sitemap в текстовом виде
    """
    pattern = 'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'
    return re.findall(pattern, content, re.MULTILINE)

def _parse_html(content):
    parser = etree.HTMLParser()
    html_tree = etree.parse(io.BytesIO(content.encode()), parser).getroot()
    xpath = './/a/@href'
    return _select_attrs(html_tree, xpath)

def _parse_xml(content):
    """ content - содержимое sitemap, возвращает EtreeElement """
    xml = _esc_amp(content)
    xml_tree = _get_nsless_xml(io.BytesIO(xml.encode()))
    xpath = 'url/loc'
    return _select_items(xml_tree, xpath)


def get_urls(sitemap, base_url):
    """ 
        sitemap - содержимое сайтмэпа str, 
        base_url - адрес сайта с протоколом http://example.com
        возвращает tuple c двумя списками 
    """
    return ([], [])