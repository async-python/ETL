from adapters.redis_adapter import ProcessStates, RedisAdapter

if __name__ == '__main__':
    redis_base = RedisAdapter()
    redis_base.set_process_state(ProcessStates.stop)
