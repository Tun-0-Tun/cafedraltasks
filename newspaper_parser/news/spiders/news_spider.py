import scrapy
from newspaper import Article
# from scrapy_splash import SplashRequest
# from quotes_js_scraper.items import QuoteItem
from scrapy_splash import SplashRequest


def parse_with_newspaper(link):
    article = Article(link)
    article.download()
    article.parse()

    if article.title:
        yield {
            "title": article.title,
            "text": article.text,
            "publish_date": article.publish_date,
            "need_render": False,

        }

        return False

    return True


class NewsSpiderSpider(scrapy.Spider):
    name = 'news_spider'

    script = '''
        function main(splash, args)
            url = args.url
            splash:go(url)
            return splash:html()
        end
    '''

    def __init__(self, link=''):
        self.link = link

    def start_requests(self):
        result = parse_with_newspaper(self.link)
        print(result)
        if result:
            yield SplashRequest(url=self.link, callback=self.parse, endpoint="execute", args={
                'lua_source': self.script
            })

    def parse(self, response):
        article = Article(self.link)

        article.download(input_html=response.text)
        article.parse()

        if article.title:
            yield {
                'title': article.title,
                'text': article.text,
                'publish_date': article.publish_date,
                'need_render': True
            }
        else:
            yield {
                'result': "It is not possible to render",
                'need_render': True
            }
