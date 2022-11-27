# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from pymongo import MongoClient
from scrapy.exceptions import DropItem

class Mongo_Pipeline(object):
    collection = 'test'

    def __init__(self):
        self.MONGODB_HOST = '104.225.140.236'
        self.MONGODB_PORT = '27017'
        self.MONGODB_USER = 'Pad36'
        self.MONGODB_PASS = '3ioM6HY8WUHKJqR4'
        # self.mongo_url = "mongodb://localhost:27017"
        self.mongo_url = 'mongodb://' + self.MONGODB_USER + ':' + self.MONGODB_PASS + '@' + self.MONGODB_HOST + ':' + self.MONGODB_PORT + '/Crudo'
        self.mongo_db = 'testdb'

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
        demandado = item.get("demandado")
        tipo = item.get("tipo")
        if self.db[self.collection].count_documents({"fecha":fecha, "expediente":expediente, "entidad":entidad,'actor':actor,'tipo':tipo,'demandado':demandado}):
            raise DropItem(" [+] Duplicate item ")
        else:
            self.db[self.collection].insert_one(dict(item))
            return item