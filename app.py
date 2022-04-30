import scrapy
import datetime as dt
import os
from influxdb import InfluxDBClient

class OilPriceSpider(scrapy.Spider):
    name = 'oilpricespider'
    start_urls = [os.environ['CHO_URL']]

    def parse(self, response):
        body = []
        time = dt.datetime.now()

        for listing in response.xpath('//div[@class="boxlisting"]'):
            dealerid = listing.xpath('.//input[@name="dealerid"]/@value').extract()[0]
            for x in listing.xpath('.//table[@class="paywithcash"]'):
                it = iter(x.xpath('tr/td/text()').getall()[1:])
                for k,v in zip(it,it):
                    out = {'time': time, 'measurement': 'oil_prices', 'tags': {'gallons': k, 'dealerid': dealerid, 'type': 'cash' }, 'fields': {'price': float(v[1:])}}
                    body.append(out)

            for x in listing.xpath('.//table[@class="paybycredit"]'):
                it = iter(x.xpath('tr/td/text()').getall()[1:])            
                for k,v in zip(it,it):
                    out = {'time': time, 'measurement': 'oil_prices', 'tags': {'gallons': k, 'dealerid': dealerid, 'type': 'credit' }, 'fields': {'price': float(v[1:])}}
                    body.append(out)

        influx_client = InfluxDBClient(os.environ['INFLUXDB_HOST'], 
						os.environ['INFLUXDB_PORT'], 
						os.environ['INFLUXDB_USER'],
						os.environ['INFLUXDB_PASSWORD'],
						os.environ['INFLUXDB_DB'])
        influx_client.write_points(body)
