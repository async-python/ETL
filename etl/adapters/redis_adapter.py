import abc
import enum
import json
from datetime import datetime
from typing import Any, Optional

from etl_decorators import backoff
from etl_settings import EtlConfig
from redis import Redis


class ProcessStates(enum.Enum):
    run = 'run'
    stop = 'stop'


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
            db=conf.redis_base
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


class RedisAdapter:
    """
    Класс для хранения состояния при работе с данными
    """

    def __init__(self, prefix: str = ''):
        self.prefix = prefix
        self.storage = RedisStorage()
        self.state = self.storage.retrieve_state()

    def set_state(self, key: str, value: Any) -> None:
        """Установить состояние для определённого ключа"""
        self.state[key] = value
        self.storage.save_state(self.state)

    def get_state(self, key: str) -> Any:
        """Получить состояние по определённому ключу"""
        return self.state.get(key)

    def set_last_time(self, value: datetime) -> None:
        """Установить время последней записи"""
        self.set_state(self.prefix + 'last_time', value.isoformat())

    def get_last_time(self) -> Optional[datetime]:
        """Получить время последней записи, если существует"""
        value = self.get_state(self.prefix + 'last_time')
        if value is not None:
            return datetime.fromisoformat(value)
        return None

    def set_process_state(self, state: ProcessStates) -> None:
        """Установить состояние работы процесса Etl"""
        self.set_state('process', state.value)

    def get_process_state(self) -> ProcessStates:
        """Получить состояние работы процесса Etl"""
        state = self.get_state('process')
        if state is None:
            return ProcessStates.stop
        return ProcessStates(self.get_state('process'))
