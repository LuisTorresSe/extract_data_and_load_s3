# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import scrapy


class ExtractItem(scrapy.Item):
    file_name = scrapy.Field()
    file_urls = scrapy.Field()
    file = scrapy.Field()