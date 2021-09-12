import logging
import sys

from pydantic import BaseSettings

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


class EtlConfig(BaseSettings):
    postgres_db: str = 'movies'
    postgres_host: str = 'localhost'
    postgres_port: int = 5432
    postgres_user: str = 'postgres'
    postgres_password: str
    postgres_options: str
    redis_host: str = 'localhost'
    redis_port: int = 6379
    redis_password: str
    elastic_host: str = 'localhost'
    elastic_port: int = 9200
    elastic_user: str = 'elastic'
    elastic_password: str
    elastic_scheme: str = 'http'
    elastic_index: str = 'movies'
    etl_butch_size: int = 10

    class Config:
        env_file = '../.env'
