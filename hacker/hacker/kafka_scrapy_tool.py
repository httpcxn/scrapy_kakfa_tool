# -*- coding: utf-8 -*-
from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.spiders import Spider
from scrapy.http import Request
import settings
from settings import SCRAPY_KAFKA_HOSTS
from settings import SCRAPY_KAFKA_TOPIC
from settings import SCRAPY_KAFKA_SPIDER_CONSUMER_GROUP
from settings import CLIENT_ID
from settings import SCRAPY_QUERRY_LENGTH
from settings import MAX_RECORDS
from kafka import KafkaConsumer


class KafkaSpiderMixin(object):


    """
    Mixin class to implement reading urls from a kafka queue.
    :type kafka_topic: str
    """


    def process_kafka_message(self, message):
        """"
        Tell this spider how to extract urls from a kafka message
        :param message: A Kafka message object
        :type message: kafka.common.OffsetAndMessage
        :rtype: str or None
        """
        if not message:
            return None

        return message.message.value

    def setup_kafka(self):

        topic = SCRAPY_KAFKA_TOPIC
        hosts = SCRAPY_KAFKA_HOSTS
        consumer_group = SCRAPY_KAFKA_SPIDER_CONSUMER_GROUP
        client_id = CLIENT_ID

        self.consumer = KafkaConsumer(topic,
                                      group_id=consumer_group,
                                      bootstrap_servers=hosts,
                                      client_id=client_id,
                                      #retry_backoff_ms=5000
                                      )

        self.crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)
        self.crawler.signals.connect(self.item_scraped, signal=signals.item_scraped)
        print(self.crawler.stats.get_stats())
        print("xxx")
        self.log("kafka_topic URLs from kafka topic '%s'" % topic)

    # 判断scrapy的scheduler队列是否满
    def is_scrapy_scheduler_query_full(self):
        # scheduler/enqueued
        if (self.crawler.stats.get_value('scheduler/enqueued') is None and self.crawler.stats.get_value('scheduler/dequeued') is None):
            return False
        elif (self.crawler.stats.get_value('scheduler/enqueued') - self.crawler.stats.get_value('scheduler/dequeued') > SCRAPY_QUERRY_LENGTH):
            return True
        else:
            return False
    def next_request(self):
        """
        按照默认 max_poll_interval_ms 300000ms 处理max_poll_records 500条
        5分钟500条，1分钟100条。
        爬虫整体处理速度：1分钟24个访问链接。这里一次拿10条
        """
        # 这种取法无法给scrapy信号量
        # for message in self.consumer:
        #     a = message
        #     print(message.value)
        #     url = message.value
        #     req = self.make_requests_from_url(url)
        #     if isinstance(req, Request):
        #         self.crawler.engine.crawl(req, spider=self)


        # if(self.crawler.stats.get_value('scheduler/enqueued')-self.crawler.stats.get_value('scheduler/dequeued')>0):
        #     print("scheduler queued is full!!!wait ...")
        #     return
        print(self.crawler.stats.get_value('scheduler/enqueued'))
        print(self.crawler.stats.get_value('scheduler/dequeued'))
        if self.is_scrapy_scheduler_query_full():
            return
        message = self.consumer.poll(max_records=MAX_RECORDS)

        # TODO 处理单条信息

        if message:
            self.log(u"xxxx")
            print(self.crawler.stats.get_stats())
            # message是
            for k in message.values():
                for i in k:
                    url = i.value
                    print(i.value)
                    req = self.make_requests_from_url(url)
                    if isinstance(req, Request):
                       self.crawler.engine.crawl(req, spider=self)
            # for sing_message in message:
            #     a = sing_message.value
            #     url = self.process_kafka_message(a)
            #     if not url:
            #         pass
            #     else:
            #         req = self.make_requests_from_url(url)
            #         if isinstance(req, Request):
            #             self.crawler.engine.crawl(req, spider=self)
            #         else:
            #             pass
        else:
            print("kong")
            pass


    def spider_idle(self):
        """Schedules a request if available, otherwise waits."""
        print("----- spdier_idle -----")
        self.next_request()
        raise DontCloseSpider

    def item_scraped(self, *args, **kwargs):
        """Avoids waiting for the spider to  idle before scheduling the next request"""
        print("----- item_scraped -----")
        self.next_request()


class ListeningKafkaSpider(KafkaSpiderMixin, Spider):

    """
    Spider that reads urls from a kafka topic when idle.
    This spider will exit only if stopped, otherwise it keeps
    listening to messages on the given topic
    Specify the topic to listen to by setting the spider's `kafka_topic`.
    Messages are assumed to be URLS, one by message. To do custom
    processing of kafka messages, override the spider's `process_kafka_message`
    method
    """

    def _set_crawler(self, crawler):
        """
        :type crawler: scrapy.crawler.Crawler
        """
        super(ListeningKafkaSpider, self)._set_crawler(crawler)
        self.setup_kafka()
