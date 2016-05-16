# -*- coding:utf-8 -*-
'''
@author: lihuxiao
'''
from multiprocessing.pool import ThreadPool
import urllib2
from bs4 import BeautifulSoup
import re
import sqlite3
import pymongo
from pymongo.mongo_client import MongoClient
import datetime
import numpy

class my_mongodb:
    
    def __init__(self):
        self.db_client = MongoClient('localhost',27017)
        self.db = self.db_client.wallstreet
        self.collection = self.db.gold
        
    def data_restore(self,list_rows):
        
        self.collection.insert_many(list_rows)
    
    def data_find(self):
        print u'we have count:',self.collection.count(),' rows'
    

def wall_parser(response):
    page_content = {}
    page_content['gold_news'] = ''
    soup = BeautifulSoup(response,'html.parser',from_encoding='utf-8')
    gold_in_page = soup.find_all('div', class_ = "summary hidden-xxs",\
                                 text=re.compile(u'([\s\S]*)黄金([\s\S]*)'))
    for gold in gold_in_page:
        #print gold.get_text()
        page_content['gold_news'] += gold.get_text()
    return page_content

if __name__ == '__main__':
    urls = ['http://wallstreetcn.com/news?status=published&type=news&order=-created_at&limit=30&page=2',
            'http://wallstreetcn.com/news?status=published&type=news&order=-created_at&limit=30&page=3',
            'http://wallstreetcn.com/news?status=published&type=news&order=-created_at&limit=30&page=4',
            'http://wallstreetcn.com/news?status=published&type=news&order=-created_at&limit=30&page=5',
            'http://wallstreetcn.com/news?status=published&type=news&order=-created_at&limit=30&page=6',
            'http://wallstreetcn.com/news?status=published&type=news&order=-created_at&limit=30&page=7',
            'http://wallstreetcn.com/news?status=published&type=news&order=-created_at&limit=30&page=8',
            'http://wallstreetcn.com/news?status=published&type=news&order=-created_at&limit=30&page=9']
    pool = ThreadPool(4)
    #zushai
    results = pool.map(urllib2.urlopen, urls)
    pool.close()
    pool.join()
    a = 1
    page_rows = []
    for result in results:
        page = wall_parser(result)
        print a ,":" + page['gold_news']
        page['page_index'] = a
        page['crawl_time'] = datetime.datetime.now()
        a += 1
        page_rows.append(page)
    mydb = my_mongodb()
    mydb.data_restore(page_rows)
    mydb.data_find()  