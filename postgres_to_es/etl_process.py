from functools import reduce

import psycopg2
from elasticsearch import Elasticsearch

from pg_base import PgBase
from etl_dataclasses import PgFilmWork
from etl_decorators import coroutine
from redis_storage import TransferState, RedisStorage
from settings import EtlConfig


class ETL:
    def __init__(self, postgres: PgBase, ):
        conf = EtlConfig()
        self.limit = conf.etl_butch_size
        self.pg_adapter = postgres
        # self.es = elastic

    def extract(self, transformer):
        last_time = self.pg_adapter.get_first_film_update_time()
        for i in range(5):
            films_obj = self.pg_adapter.get_films_ids(last_time, self.limit)
            films_ids = tuple([obj.id for obj in films_obj])
            last_time = films_obj[-1].updated_at
            transformer.send(films_ids)

    @coroutine
    def transform(self):
        while films_ids := (yield):
            films = self.pg_adapter.get_films_by_id(films_ids)
            print(films)

    def load(self):
        pass

    def __call__(self, *args, **kwargs):
        transformer = self.transform()
        self.extract(transformer)


if __name__ == '__main__':
    # transfer_state = TransferState(RedisStorage())
    pg_base = PgBase()
    # es_base = Elasticsearch(EtlConfig.es_dsn)
    etl = ETL(pg_base)
    etl()
