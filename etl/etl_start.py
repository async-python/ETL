from adapters.elastic_adapter import EsAdapter
from adapters.postgres_adapter import PgAdapter
from adapters.redis_adapter import RedisAdapter
from etl_exceptions import (DataDoesNotExistException, EmptyStartTimeException,
                            ExceededConnectLimitException)
from etl_process import Etl
from etl_settings import logger, EtlConfig

if __name__ == '__main__':
    try:
        conf = EtlConfig()
        etl_movies = Etl(
            PgAdapter(conf.postgres_table_films),
            RedisAdapter(conf.postgres_table_films),
            EsAdapter(conf.elastic_index_film)
        )
        etl_genres = Etl(
            PgAdapter(conf.postgres_table_genre),
            RedisAdapter(conf.postgres_table_genre),
            EsAdapter(conf.elastic_index_genre)
        )
        etl_persons = Etl(
            PgAdapter(conf.postgres_table_person),
            RedisAdapter(conf.postgres_table_person),
            EsAdapter(conf.elastic_index_person)
        )
        etl_movies()
        etl_genres()
        etl_persons()
    except ExceededConnectLimitException as error:
        logger.error(error)
    except EmptyStartTimeException as error:
        logger.error(error)
    except DataDoesNotExistException as error:
        logger.error(error)
    except Exception as error:
        logger.error(error)
