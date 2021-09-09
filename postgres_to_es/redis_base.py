import abc
import json
from datetime import datetime
from typing import Any, Optional

from redis import Redis

from etl_decorators import backoff
from etl_settings import EtlConfig


class BaseStorage:
    @abc.abstractmethod
    def save_state(self, state: dict) -> None:
        """Сохранить состояние в постоянное хранилище"""
        pass

    @abc.abstractmethod
    def retrieve_state(self) -> dict:
        """Загрузить состояние локально из постоянного хранилища"""
        pass


class RedisStorage(BaseStorage):
    def __init__(self):
        conf = EtlConfig()
        self.redis_adapter = Redis(
            host=conf.redis_host,
            port=conf.redis_port,
            password=conf.redis_password,
            decode_responses=True,
        )

    @backoff()
    def save_state(self, state: dict) -> None:
        self.redis_adapter.set('data', json.dumps(state))

    @backoff()
    def retrieve_state(self) -> dict:
        raw_data = self.redis_adapter.get('data')
        if raw_data is None:
            return {}
        return json.loads(raw_data)


class RedisState:
    """
    Класс для хранения состояния при работе с данными, чтобы постоянно
    не перечитывать данные с начала.
    Здесь представлена реализация с сохранением состояния в файл.
    В целом ничего не мешает поменять это поведение на работу с БД или
    распределённым хранилищем.
    """

    def __init__(self, storage: BaseStorage):
        self.storage = storage
        self.state = self._retrieve_state()

    def _retrieve_state(self) -> dict:
        data = self.storage.retrieve_state()
        if not data:
            return {}
        return data

    def _set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.state[key] = value
        self.storage.save_state(self.state)

    def _get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        return self.state.get(key)

    def set_last_time(self, value: datetime) -> None:
        self._set_state('last_time', value.isoformat())

    def get_last_time(self) -> Optional[datetime]:
        value = self._get_state('last_time')
        if value is not None:
            return datetime.fromisoformat(value)
        return None
