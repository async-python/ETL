import uuid
from functools import reduce
from typing import Coroutine, List

from etl_dataclasses import PgFilmWork
from etl_decorators import coroutine
from etl_settings import EtlConfig, logger
from pg_base import PgBase
from postgres_to_es.es_schema import INDEX_SCHEMA
from redis_base import RedisState, RedisStorage

from postgres_to_es.es_base import EsBase


class ETL:
    def __init__(
            self, postgres: PgBase, redis: RedisState, elastic: EsBase):
        conf = EtlConfig()
        self.limit = conf.etl_butch_size
        self.pg_adapter = postgres
        self.redis_adapter = redis
        self.es_adapter = elastic

    def extract(self, transformer: Coroutine) -> (uuid,):
        logger.info('Процесс ETL запущен')
        for i in range(1):
            last_time = (self.redis_adapter.get_last_time() or
                         self.pg_adapter.get_first_film_update_time())
            films_obj = self.pg_adapter.get_films_ids(last_time, self.limit)
            films_ids = tuple([obj.id for obj in films_obj])
            self.redis_adapter.set_last_time(films_obj[-1].updated_at)
            transformer.send(films_ids)

    @coroutine
    def transform(self, loader: Coroutine):
        while films_ids := (yield):
            films = self.pg_adapter.get_films_by_id(films_ids)
            loader.send(films)

    @coroutine
    def load(self) -> None:
        while extracted_data := (yield):
            films: List[PgFilmWork] = extracted_data
            print([film.id for film in films])
            try:
                self.es_adapter.es.indices.delete('movies')
                self.es_adapter.create_index('movies', INDEX_SCHEMA)
                # self.es_adapter.bulk_update()
                # print(self.es_adapter.es.get('movies', '1'))
            except Exception as e:
                print(e)

    def __call__(self, *args, **kwargs):
        return reduce(lambda val, func: func(val),
                      [self.load(), self.transform, self.extract])


if __name__ == '__main__':
    redis_base = RedisState(RedisStorage())
    pg_base = PgBase()
    es_base = EsBase()
    etl = ETL(pg_base, redis_base, es_base)
    etl()
