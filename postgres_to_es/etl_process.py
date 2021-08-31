import os

from dotenv import load_dotenv
from elasticsearch import Elasticsearch

from decorators import backoff

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
        self.es = Elasticsearch()

    @backoff()
    def extract(self):
        self.es.indices.create(index='table3', ignore=400)
        return self.es.count(index='table3')

    def transform(self):
        pass

    def load(self):
        pass


if __name__ == '__main__':
    etl = ETL()
    print(etl.extract())
    print(etl.extract())
    # dsl = {'dbname': DB_NAME, 'user': DB_USER, 'password': DB_PASSWORD,
    #        'host': DB_HOST, 'port': DB_PORT, 'options': DB_OPTIONS, }
    # r = redis.Redis(host='localhost', port=6379, db=0)
    # r.set('foo', 'pizdulkin')
    # print(r.get('foo').decode())
    # pg_conn = backoff(psycopg2.connect(**dsl, cursor_factory=DictCursor))
