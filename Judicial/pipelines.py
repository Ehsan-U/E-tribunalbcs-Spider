# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
import datetime
from isodate import parse_datetime
from itemadapter import ItemAdapter
from pymongo import MongoClient
from scrapy.exceptions import DropItem

class Mongo_Pipeline(object):
    collection = 'test'

    def __init__(self):
        self.MONGODB_HOST = '104.225.140.236'
        self.MONGODB_PORT = '27017'
        self.MONGODB_USER = 'Pad32'
        self.MONGODB_PASS = 'lGhg4S8AYZ85o7qe'
        self.mongo_url = 'mongodb://' + self.MONGODB_USER + ':' + self.MONGODB_PASS + '@' + self.MONGODB_HOST + ':' + self.MONGODB_PORT + '/Crudo'
        # self.mongo_url = 'mongodb+srv://window:VYEubgqPlDsQwwoZ@cluster1.409m5rv.mongodb.net/?retryWrites=true&w=majority'
        # self.mongo_db = 'testdb'

    def open_spider(self, spider):
        self.client = MongoClient(self.mongo_url)
        self.db = self.client[self.mongo_db]

    def close_spider(self, spider):
        self.client.close()

    def process_item(self, item, spider):
        fecha = item.get("fecha")
        expediente = item.get("expediente")
        entidad = item.get("entidad")
        actor = item.get("actor")
        tipo = item.get("tipo")
        
        if self.db[self.collection].count_documents({"fecha":fecha, "expediente":expediente, "entidad":entidad,'actor':actor,'tipo':tipo}):
            raise DropItem(" [+] Duplicate item ")
        else:
            fecha = item['fecha']
            today = datetime.datetime.now()
            fecha_insercion = parse_datetime(today.isoformat())
            fecha_tecnica = parse_datetime(datetime.datetime.strptime(fecha, "%Y/%m/%d").isoformat())
            item['fecha_insercion'] = fecha_insercion
            item['fecha_tecnica'] = fecha_tecnica
            self.db[self.collection].insert_one(dict(item))
            return item

