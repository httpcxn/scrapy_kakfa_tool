# -*- coding: utf-8 -*-
from scrapy import signals
from scrapy.exceptions import DontCloseSpider
from scrapy.spiders import Spider
from scrapy.http import Request
import settings
from settings import SCRAPY_KAFKA_HOSTS
from settings import SCRAPY_KAFKA_TOPIC
from settings import SCRAPY_KAFKA_SPIDER_CONSUMER_GROUP
from kafka import SimpleClient
from kafka import KafkaConsumer

from kafka.consumer import SimpleConsumer

class KafkaSpiderMixin(object):

    """
    Mixin class to implement reading urls from a kafka queue.
    :type kafka_topic: str
    """
    # kafka_topic = None

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

    def setup_kafka(self, settings):
        """Setup redis connection and idle signal.
        This should be called after the spider has set its crawler object.
        :param settings: The current Scrapy settings being used
        :type settings: scrapy.settings.Settings
        """
        # if not hasattr(self, 'topic') or not self.topic:
        #     self.topic = '%s-starturls' % self.name
        topic = SCRAPY_KAFKA_TOPIC
        hosts = SCRAPY_KAFKA_HOSTS
        consumer_group = SCRAPY_KAFKA_SPIDER_CONSUMER_GROUP
        try:
            kafka = SimpleClient(hosts)

        except Exception as e:
            print (e)
        # wait at most 1sec for more messages. Otherwise continue

        self.consumer = SimpleConsumer(kafka, consumer_group, topic,
                                       auto_commit=True, #iter_timeout=1.0)
                                       )
        # idle signal is called when the spider has no requests left,
        # that's when we will schedule new requests from kafka topic
        self.crawler.signals.connect(self.spider_idle, signal=signals.spider_idle)
        self.crawler.signals.connect(self.item_scraped, signal=signals.item_scraped)
        self.log("kafka_topic URLs from kafka topic '%s'" % topic)

    def next_request(self):
        """
        Returns a request to be scheduled.
        :rtype: str or None
        """
        message = self.consumer.get_message(True)
        url = self.process_kafka_message(message)
        if not url:
            return None
        try:
            req = self.make_requests_from_url(url)
            return req
        except Exception as e:
            print(e)
            return None
        # return self.make_requests_from_url(url)

    def schedule_next_request(self):
        """Schedules a request if available"""
        req = self.next_request()
        # 这里要对req进行判断，如果是Request就发送到scrapy抓取
        if isinstance(req, Request):
            self.crawler.engine.crawl(req, spider=self)

    def spider_idle(self):
        """Schedules a request if available, otherwise waits."""
        self.schedule_next_request()
        raise DontCloseSpider

    def item_scraped(self, *args, **kwargs):
        """Avoids waiting for the spider to  idle before scheduling the next request"""
        self.schedule_next_request()


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
        self.setup_kafka(settings)