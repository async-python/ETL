from adapters.elastic_adapter import EsAdapter
from adapters.postgres_adapter import PgAdapter
from adapters.redis_adapter import RedisAdapter
from etl_exceptions import (DataDoesNotExistException, EmptyStartTimeException,
                            ExceededConnectLimitException)
from etl_process import Etl
from etl_settings import logger

if __name__ == '__main__':
    try:
        etl_movies = Etl(
            PgAdapter('film_work'), RedisAdapter('f_'), EsAdapter('movie'))
        etl_genres = Etl(
            PgAdapter('genre'), RedisAdapter('g_'), EsAdapter('genre'))
        etl_persons = Etl(
            PgAdapter('person'), RedisAdapter('p_'), EsAdapter('person'))
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
