from redis_base import ProcessStates, RedisState, RedisStorage

if __name__ == '__main__':
    redis_base = RedisState(RedisStorage())
    redis_base.set_process_state(ProcessStates.stop)
