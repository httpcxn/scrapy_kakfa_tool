# -*- coding: utf-8 -*-

import scrapy
from ..kafka_scrapy_tool import ListeningKafkaSpider
class baiduSpider(ListeningKafkaSpider):
    name = "hacker"
    # start_urls = [
    #     "http://company.zhaopin.com/CC302752887.htm"
    # ]

    def parse(self, response):
        print("start")
        self.logger.info(u"开始抓取.")
        a = response.url

        print(a)
        print(u"访问链接成功")
