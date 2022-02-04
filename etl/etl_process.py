from datetime import datetime
from functools import reduce
from typing import Any, Callable, Coroutine, List, Optional

from adapters.elastic_adapter import EsAdapter
from adapters.postgres_adapter import PgAdapter
from adapters.redis_adapter import ProcessStates, RedisAdapter
from etl_dataclasses import PgObjID
from etl_decorators import coroutine
from etl_exceptions import (EmptyStartTimeException,
                            ProcessAlreadyExistException, ZeroPgRowsException)
from etl_settings import EtlConfig, logger


class TimeManager:
    """
    Предоставляет интерфейс для получения актуальной даты начала Etl процесса
    """

    def __init__(self, postgres: PgAdapter, redis: RedisAdapter) -> None:
        self.pg_adapter = postgres
        self.redis_adapter = redis

    def get_last_time(self) -> datetime:
        time = (self.redis_adapter.get_last_time() or
                self.pg_adapter.get_first_update_time())
        if time is None:
            raise EmptyStartTimeException
        return time


class LogHelper:
    """Вычисляет процент загрузки данных и выводит в лог"""

    def __init__(self, rows_limit: int, pg_adapter: PgAdapter,
                 time_manager: TimeManager) -> None:
        self.pg_adapter = pg_adapter
        self.butch_size = rows_limit
        self.time_manager = time_manager
        self.output_percent_value = 10
        self.start_time = None
        self.total_rows = None
        self.log_output_step = None
        self.total_rows_loaded = 0
        self.rows_counter = 0
        self._define_settings()

    def _define_settings(self):
        self.start_time = self.time_manager.get_last_time()
        self.total_rows = self.pg_adapter.get_rows_count(self.start_time)
        self.log_output_step = round(
            self.total_rows / self.output_percent_value, 0)

    def update_logger_conf(self) -> None:
        """Обновление параметров вывода логов в процессе загрузки данных"""
        self.total_rows_loaded += self.butch_size
        self.rows_counter += self.butch_size
        self.total_rows = self.pg_adapter.get_rows_count(self.start_time)

    def output_log(self) -> None:
        """Вывод лога"""
        if self.rows_counter >= self.log_output_step:
            percent = min(round(
                100 * self.total_rows_loaded / self.total_rows, 0), 100)
            logger.info(f'Записано {percent}% данных')
            self.rows_counter = 0

    def __call__(self, *args, **kwargs) -> None:
        self.update_logger_conf()
        self.output_log()


class Etl:
    """
    Pipeline для выгрузки данных из Postgres в Elasticsearch
    завершает работу, если данных для загрузки нет
    """

    def __init__(self, postgres: PgAdapter,
                 redis: RedisAdapter, elastic: EsAdapter):
        conf = EtlConfig()
        self.rows_limit = conf.etl_butch_size
        self.pg_adapter = postgres
        self.redis_adapter = redis
        self.es_adapter = elastic
        self.log_helper = None
        self.time_manager = None
        self.temp_time_value = None

    def _define_settings(self):
        self.time_manager = TimeManager(self.pg_adapter, self.redis_adapter)
        self.log_helper = LogHelper(
            self.rows_limit, self.pg_adapter, self.time_manager)

    def extract(self, transformer: Coroutine):
        """Получение списка ID кинопроизведений"""
        while self.redis_adapter.get_process_state() == ProcessStates.run:
            limited_ids: list[PgObjID] = self.pg_adapter.get_data_ids(
                self.time_manager.get_last_time(), self.rows_limit)
            if len(limited_ids):
                films_ids = tuple([obj.id for obj in limited_ids])
                self.temp_time_value = limited_ids[-1].updated_at
                transformer.send(films_ids)
            else:
                self.redis_adapter.set_process_state(ProcessStates.stop)
        logger.info(f'Etl завершен для таблицы: {self.pg_adapter.table_name}')

    @coroutine
    def transform(self, loader: Coroutine):
        """
        Получение перечня кинопроизведений по ID из Postgres и преобразование
        в str для загрузки в Elasticsearch
        """
        while films_ids := (yield):
            data: List[Any] = self.pg_adapter.get_data_by_ids(
                films_ids)
            loader.send(data)

    @coroutine
    def load(self):
        """Загрузка данных в Elasticsearch"""
        while extracted_data := (yield):
            data: List[Any] = extracted_data
            self.es_adapter.bulk_create(data)
            self.redis_adapter.set_last_time(self.temp_time_value)
            self.log_helper()

    def __call__(self, *args, **kwargs) -> Optional[Callable]:
        if self.redis_adapter.get_process_state() == ProcessStates.run:
            raise ProcessAlreadyExistException
        try:
            self._define_settings()
        except ZeroPgRowsException as error:
            logger.warning(error)
            return
        self.redis_adapter.set_process_state(ProcessStates.run)
        logger.info(
            f'Etl запущен для таблицы: {self.pg_adapter.table_name}')
        return reduce(lambda val, func: func(val),
                      [self.load(), self.transform, self.extract])
