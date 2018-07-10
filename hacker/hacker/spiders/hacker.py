# -*- coding: utf-8 -*-

import scrapy
from ..kafka_scrapy_tool import ListeningKafkaSpider
from time import sleep
class baiduSpider(ListeningKafkaSpider):
    name = "hacker"
    # start_urls = [
    #     "http://company.zhaopin.com/CC302752887.htm"
    # ]

    def parse(self, response):
        #print("start")
        #self.logger.info(u"开始抓取.")
        a = response.url
        # sleep(5)
        print(self.crawler.stats.get_stats())
        print(self.crawler.stats.get_value('scheduler/enqueued'))
        print(self.crawler.stats.get_value('scheduler/dequeued'))
        # print(a)
        print(u"访问链接成功")
