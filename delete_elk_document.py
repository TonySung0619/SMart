from elasticsearch import Elasticsearch
from read_db_credentials import *

url = es_host
es = Elasticsearch(es_host)

# 刪除index為'iii',id為'i'的資料
for i in range(2319335, 2323793):
    es.delete(index='iii', id=i)

