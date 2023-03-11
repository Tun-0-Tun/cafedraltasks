from scrapy import cmdline

cmdline.execute("scrapy crawl ok "
                "-a link=https://ok.ru/ria "
                "-a start_date=2020-01-01 "
                "-a end_date=2023-12-30 "
                "-a iterations=8 "
                "-o output.json".split())

# scrapy crawl myspider -a parameter1=value1 -a parameter2=value2
