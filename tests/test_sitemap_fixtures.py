import pytest

@pytest.fixture
def urls_list():
    return ['https://yandex.ru/blog/yandexbrowser',
            'https://yandex.ru/blog/yandexbrowser?year=2013&month=Dec',
            'https://yandex.ru/blog/yandexbrowser?year=2013&month=Nov',
            'https://yandex.ru/blog/yandexbrowser?year=2013&month=Oct',
            'https://yandex.ru/blog/yandexbrowser?year=2013&month=Sep']


@pytest.fixture
def txt_sitemap():
    return """
        https://yandex.ru/blog/yandexbrowser
        https://yandex.ru/blog/yandexbrowser?year=2013&month=Dec
        https://yandex.ru/blog/yandexbrowser?year=2013&month=Nov
        https://yandex.ru/blog/yandexbrowser?year=2013&month=Oct
        https://yandex.ru/blog/yandexbrowser?year=2013&month=Sep
        """

@pytest.fixture
def html_sitemap():
    return """
        <!DOCTYPE html>
        <html lang="en">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <meta http-equiv="X-UA-Compatible" content="ie=edge">
            <title>Document</title>
        </head>
        <body>
            <p>Lorem ipsum dolor sit amet, <a href="https://yandex.ru/blog/yandexbrowser">consectetur adipiscing elit</a>. Quisque eleifend ex vel ligula maximus, 
            sed finibus ex feugiat. In consectetur diam at tellus pulvinar, eget dignissim nisl efficitur. 
            Nunc a quam sed erat luctus maximus aliquet nec ante. Fusce ipsum diam, gravida a semper a, sollicitudin sit amet lacus.</p> 
            <p>Proin cursus, sapien sed facilisis maximus, <a href="blog/yandexbrowser?year=2013&month=Dec">neque arcu facilisis</a> eros, 
            nec semper urna dolor vel lacus. Vestibulum mattis <a href="//yandex.ru/blog/yandexbrowser?year=2013&month=Oct">erat quis</a>
            sagittis rutrum. Nunc fermentum justo tincidunt tempor blandit. <a href="/blog/yandexbrowser?year=2013&month=Sep">Suspendisse</a> hendrerit sagittis euismod.</p> 
        </body>
        </html>
        """

@pytest.fixture
def xml_sitemap():
    return """
        <urlset 
            xmlns="http://www.sitemaps.org/schemas/sitemap/0.9" 
            xmlns:news="http://www.google.com/schemas/sitemap-news/0.9" 
            xmlns:xhtml="http://www.w3.org/1999/xhtml" 
            xmlns:mobile="http://www.google.com/schemas/sitemap-mobile/1.0" 
            xmlns:image="http://www.google.com/schemas/sitemap-image/1.1">
            <url>
                <loc>https://yandex.ru/blog/yandexbrowser</loc>
            </url>
            <url>
                <loc> https://yandex.ru/blog/yandexbrowser?year=2013&month=Dec</loc>
            </url>
            <url>
                <loc> https://yandex.ru/blog/yandexbrowser?year=2013&month=Nov
                </loc>
            </url>
            <url>
                <loc> https://yandex.ru/blog/yandexbrowser?year=2013&month=Oct
                </loc>
            </url>
            <url>
                <loc> https://yandex.ru/blog/yandexbrowser?year=2013&month=Sep
                </loc>
            </url>
        </urlset>
        """

@pytest.fixture
def xml_sitemap_clean():
    return """
        <urlset>
            <url>
                <loc>https://yandex.ru/blog/yandexbrowser</loc>
            </url>
            <url>
                <loc> https://yandex.ru/blog/yandexbrowser?year=2013&amp;month=Dec</loc>
            </url>
            <url>
                <loc> https://yandex.ru/blog/yandexbrowser?year=2013&amp;month=Nov
                </loc>
            </url>
            <url>
                <loc> https://yandex.ru/blog/yandexbrowser?year=2013&amp;month=Oct
                </loc>
            </url>
            <url>
                <loc> https://yandex.ru/blog/yandexbrowser?year=2013&amp;month=Sep
                </loc>
            </url>
        </urlset>
        """