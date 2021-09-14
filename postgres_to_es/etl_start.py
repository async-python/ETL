from es_base import EsBase
from etl_exceptions import (ExceededConnectLimitException,
                            EmptyStartTimeException)
from etl_process import ETL
from etl_settings import logger
from pg_base import PgBase
from redis_base import RedisState, RedisStorage

if __name__ == '__main__':
    try:
        redis_base = RedisState(RedisStorage())
        pg_base = PgBase()
        es_base = EsBase()
        etl = ETL(pg_base, redis_base, es_base)
        etl()
    except ExceededConnectLimitException as error:
        logger.error(error)
    except EmptyStartTimeException as error:
        logger.error(error)
