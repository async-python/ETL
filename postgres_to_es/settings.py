from pydantic import BaseSettings


class EtlConfig(BaseSettings):
    postgres_db: str
    postgres_host: str
    postgres_port: int
    postgres_user: str
    postgres_password: str
    postgres_schema: str
    redis_host: str
    redis_port: int
    redis_password: str
    elastic_host: str
    elastic_port: int
    elastic_user: str
    elastic_password: str
    elastic_index: str
    etl_butch_size: int = 10

    class Config:
        env_file = '../.env'


print(EtlConfig().dict())
