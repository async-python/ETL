from adapters.redis_adapter import ProcessStates, RedisAdapter
from etl_exceptions import ProcessAlreadyExistException

if __name__ == '__main__':
    redis_base = RedisAdapter()
    if redis_base.get_process_state() == ProcessStates.run:
        raise ProcessAlreadyExistException
    redis_base.set_process_state(ProcessStates.run)
