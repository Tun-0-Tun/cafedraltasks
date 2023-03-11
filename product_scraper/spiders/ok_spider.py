import json
import re
import time
from datetime import datetime, date, timedelta

import scrapy
# group_profile = {
#     "url": str,
#     "group_name": str,
#     "group_id": str,
#     "description": str,
#     "members_count": int,
#     "posts_count": str,
#     "place": str,
#     "item_type": "GroupProfile"
# }
#
# group_post = {
#     "url": str,
#     "user_id": str,
#     "text": str,
#     "publication_date": str,
#     "comments_count": int,
#     "likes_count": int,
#     "item_type": "GroupPost"
# }


class OKSpider(scrapy.Spider):
    name = "ok"

    def __init__(self, link='', start_date='2000-01-01', end_date='2023-12-30', iterations=10, **kwargs):
        self.group_id = ""
        self.next_token = ""
        self.link = link

        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")  # дата начала
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")  # дата начала
        self.comm_num = -1  # ключ по количеству постов
        self.like_num = -1  # ключ по количеству лайков
        self.item_pipeline = 0  # ключ по наличию текста
        self.item_group = 0  # ключ по наличию place

        self.iterations = int(iterations)
        self.links, self.titles, self.dates, self.urls, self.like_counts, self.comment_counts, self.place_counts, self.profile_ids = [], [], [], [], [], [], [], []
        super().__init__(self.name)

    def remove_duplicates(self, seq):
        seen = set()
        seen_add = seen.add
        return [x for x in seq if not (x in seen or seen_add(x))]

    def start_requests(self):
        yield scrapy.Request(self.link, self.parse_init_)

    def parse_init_(self, response):
        # Парсинг первых страниц
        text = response.text
        title = response.xpath("head/title/text()").get()
        group_id = set(re.findall(r"(?<=groupid=)\d+", response.text.lower())).pop()
        description = response.xpath(r"head/meta[@name='description']/@content")[0].get()
        members_count = re.findall(r'(?<=groupMembersCntEl">)\d+.*\d+(?=<)', response.text).pop()
        place = response.xpath(r'//*[@id="hook_Block_LeftColumnTopCardAltGroup"]/div/div[2]/div/div[4]/div/div[2]/div[2]/text()').get()  # null check!!!
        post_count = response.xpath('//*[@id="hook_Block_MiddleColumnTopCard_MenuAltGroup"]/nav/a[2]/span/text()').get()

        group_profile = {
            "url": self.link,
            "group_name": title.split(" | ")[0],
            "group_id": group_id,
            "description": description,
            "members_count": int(members_count.replace("&nbsp;", "")),
            "posts_count": int(post_count.replace('\xa0', '')),
            "place": place,
            "item_type": "GroupProfile"
        }

        yield group_profile

        for post in response.xpath('//span[@class="widget_cnt controls-list_lk js-klass js-klass-action"]'):
            self.links.append(f"{self.link}/topic/{post.get().split(':')[1]}")

        for i in response.xpath("//div[starts-with(@data-link-source, '') and string-length(@data-link-source) > 0]"):
            post_title = scrapy.Selector(text=i.extract()).xpath('//div[@class="media-text_cnt_tx emoji-tx textWrap"]/text()').get()
            self.titles.append(post_title)

        for post_date in response.xpath('//div[@class="feed_date"]/text()'):
            self.dates.append(post_date.get())

        count = 3
        for post_date in response.xpath('//span[@class="widget_count js-count"]/text()'):
            if count % 3 == 2:
                self.like_counts.append(post_date.get().replace("\xa0", ""))

            count += 1

        count = 3
        for post_date in response.xpath('//span[@class="widget_count js-count"]/text()'):
            if count % 3 == 0:
                self.comment_counts.append(post_date.get().replace("\xa0", ""))

            count += 1

        for post_date in response.xpath('//span[@class="media-block media-link__v2  __place-wide"]/text()'):
            self.place_counts.append(post_date.get())

        self.next_token = re.findall(r'(?<=st.markerB=")\w+(?=")', text).pop()
        self.group_id = re.findall(r'(?<=groupId=)\d+(?=")', text).pop()
        self.gwt = re.findall(r'(?<=gwtHash:")\w+(?=")', text).pop()

        self.profile_ids.extend(map(lambda item: int(re.findall(r"\d+", item).pop()), response.xpath('//a[@class="user-link o"]/@href').extract()))

        for page in range(self.iterations):
            yield scrapy.FormRequest(f"https://ok.ru/dk?cmd=AltGroupMainFeedsNewRB&st.gid={self.group_id}", formdata={
                "fetch": "false",
                "st.page": str(page),
                "st.gid": str(self.group_id),
                "st.markerB": str(self.next_token),
                "gwt.requested": str(self.gwt),
            }, callback=self.parse)

            time.sleep(1)

        months = ['Jan', 'Feb', 'March', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sept', 'Oct', 'Nov', 'Dec']
        lengths = list(map(len, (self.comment_counts, self.dates, self.links, self.titles, self.like_counts)))

        # Вывод данных
        for i in range(min(lengths)):
            current_date = self.dates[i]

            if ':' in current_date:
                if "yesterday" in current_date:
                    required_date = date.today() - timedelta(days=1)
                    hours, minutes = map(int, current_date.split(" ")[-1].split(":"))

                else:
                    required_date = date.today()
                    hours, minutes = map(int, current_date.split(":"))

                current_date = datetime(required_date.year, required_date.month, required_date.day, hours, minutes)

            else:
                current_date = current_date.split()
                month = months.index(current_date[1]) + 1

                current_date = datetime(int(current_date[-1]), month, int(current_date[0]), 12, 0, 0)

            passed_date = self.start_date <= current_date <= self.end_date
            passed_likes = self.like_num in (-1, self.like_counts[i])
            passed_comments = self.comm_num in (-1, self.comment_counts[i])

            if passed_date and passed_comments and passed_likes:
                yield {
                    "url": self.links[i],
                    "user_id": self.profile_ids[i] if i < len(self.profile_ids) else self.group_id,
                    "text": self.titles[i],
                    "publication_date": current_date.timestamp(),
                    "comments_count": int(self.comment_counts[i]),
                    "likes_count": int(self.like_counts[i]),
                    "item_type": "GroupPost"
                }

        # json.dump(res_array, open("../output_posts.json", "w", encoding="utf-8"), ensure_ascii=False, indent=4)

    def parse(self, response):
        # Парсинг страниц, начиная с первого скролла

        self.next_token = re.findall(r'(?<=st.markerB=").*(?=")', response.text).pop()
        self.profile_ids.extend(map(lambda item: int(re.findall(r"\d+", item).pop()), response.xpath('//a[@class="user-link o"]/@href').extract()))

        for post in response.xpath('//span[@class="widget_cnt controls-list_lk js-klass js-klass-action"]'):
            self.links.append(self.link + "/topic/" + post.get().split(':')[1])

        for i in response.xpath("//div[starts-with(@data-link-source, '') and string-length(@data-link-source) > 0]"):
            post_title = scrapy.Selector(text=i.extract()).xpath('//div[@class="media-text_cnt_tx emoji-tx textWrap"]/text()').get()
            self.titles.append(post_title)

        for post_date in response.xpath('//div[@class="feed_date"]/text()'):
            self.dates.append(post_date.get())

        count = 3
        for post_date in response.xpath('//span[@class="widget_count js-count"]/text()'):
            if count % 3 == 2:
                self.like_counts.append(post_date.get().replace("\xa0", ""))

            count += 1

        count = 3
        for post_date in response.xpath('//span[@class="widget_count js-count"]/text()'):
            if count % 3 == 0:
                self.comment_counts.append(post_date.get().replace("\xa0", ""))
            count += 1
        for post_date in response.xpath('//span[@class="media-block media-link__v2  __place-wide"]/text()'):
            self.place_counts.append(post_date.get())
