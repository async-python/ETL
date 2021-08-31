import os

import psycopg2 as psycopg2
import redis
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from psycopg2.extras import DictCursor

from utils import backoff

load_dotenv()

DB_NAME = os.environ.get('POSTGRES_DB')
DB_USER = os.environ.get('POSTGRES_USER')
DB_PASSWORD = os.environ.get('POSTGRES_PASSWORD')
DB_HOST = os.environ.get('DB_HOST_DEV')
DB_PORT = os.environ.get('DB_PORT')
DB_OPTIONS = os.environ.get('DB_OPTIONS')
REDIS_HOST = os.environ.get('REDIS_HOST')
REDIS_PORT = os.environ.get('REDIS_PORT')
ES_HOST = os.environ.get('ES_HOST')
ES_PORT = os.environ.get('ES_PORT')


class ETL:
    def __init__(self):
        pass

    def extract(self):
        pass

    def transform(self):
        pass

    def load(self):
        pass


if __name__ == '__main__':
    dsl = {'dbname': DB_NAME, 'user': DB_USER, 'password': DB_PASSWORD,
           'host': DB_HOST, 'port': DB_PORT, 'options': DB_OPTIONS, }
    es = Elasticsearch()
    r = redis.Redis(host='localhost', port=6379, db=0)
    r.set('foo', 'pizdulkin')
    print(es.count(index='table'))
    print(r.get('foo').decode())
    pg_conn = backoff(psycopg2.connect(**dsl, cursor_factory=DictCursor))
