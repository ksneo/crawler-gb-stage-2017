import urllib.request
from lxml import etree, objectify
import io
import re

class Sitemap:
    def __init__(self, url):
        self.url = url

def esc_amp(text):
    """ text строка, возвращает строку с замененными & """ 
    # замена & на &amp;
    return re.sub(r'&([^a-zA-Z#])',r'&amp;\1', text)

def get_nsless_xml(xml):
    """ xml - bytes[] возращает root ETreeElement """
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

def load_sitemap(url):
    """ url - адрес sitemap, возвращает EtreeElement """
    with urllib.request.urlopen(url) as response:
        xml = esc_amp(response.read().decode())
    return get_nsless_xml(io.BytesIO(xml.encode()))

def get_urls(xml_elem, xpath):
    """ xml_elem ETreeElement, xpath - путь поиска, возвращает список урлов в элементе """
    urls = [x.text.strip() for x in xml_elem.xpath(xpath)]
    return urls
