import psycopg2
from elasticsearch import Elasticsearch

from redis_storage import TransferState, RedisStorage
from settings import EtlConfig


class ETL:
    def __init__(self, state: TransferState, postgres: psycopg2.connect,
                 elastic: Elasticsearch):
        self.state = state
        self.postgres = postgres
        self.es = elastic

    def extract(self):
        self.es.indices.create(index='table3', ignore=400)
        return self.es.count(index='table3')

    def transform(self):
        pass

    def load(self):
        pass

    def __call__(self, *args, **kwargs):
        print(self.extract())


if __name__ == '__main__':
    transfer_state = TransferState(RedisStorage())
    pg_conn = psycopg2.connect(dsn=EtlConfig.postgres_dsn)
    es_conn = Elasticsearch(EtlConfig.es_dsn)
    etl = ETL(transfer_state, pg_conn, es_conn)
    etl()
