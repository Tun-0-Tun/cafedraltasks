from scrapy import cmdline

cmdline.execute("scrapy crawl news_spider "
                "-a link=http://quotes.toscrape.com/js "
                "-o output.json".split())
me