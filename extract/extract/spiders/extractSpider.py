from typing import Any, Iterable
import scrapy
from pathlib import Path
from scrapy.http import Request, Response
import re
import boto3

class ExtractSpider(scrapy.Spider):
    name = "extract_data"


    def start_requests(self):
        url =  "https://www.datosabiertos.gob.pe/dataset/%C3%B3rdenes-de-servicios-realizadas-trav%C3%A9s-de-los-cat%C3%A1logos-electr%C3%B3nicos-central-de-compras"
        yield scrapy.Request(url)

    def parse(self, response):
        def extract_name(query):
            patron = r'Reporte.*'
            coincidencia = re.search(patron, query)
            return coincidencia.group()
        
      
        list_object = self.getListObjectBucketDir(self.bucket_name, self.access_key, self.secret_key)
        print(list_object)
        for link in response.css(".resource-list li:not(.first) .links "):
            name = extract_name(link.css(".data-link::attr(href)").get())
            links = link.css(".data-link::attr(href)").get()
            if name not in list_object:
                yield {
                    "file_name": name,
                    "file_urls": [links]
                }

    def getListObjectBucketDir(self, bucket_name, access_key, secret_key):
        listObjects = []
        s3_client = boto3.client('s3', aws_access_key_id = access_key, aws_secret_access_key = secret_key )
        response = s3_client.list_objects_v2(Bucket = bucket_name)
        
        if 'Contents' in response :
            objects = response['Contents']
        for object in objects:
            listObjects.append(object['Key'].split('/')[1])
        return listObjects
