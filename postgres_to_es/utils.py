import time
from functools import wraps


def backoff(
        cls_connection, start_sleep_time=0.1, factor=2,
        border_sleep_time=10):
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка. Использует наивный экспоненциальный рост времени
    повтора (factor) до граничного времени ожидания (border_sleep_time)
    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param cls_connection:
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            repeats = 0
            delay = start_sleep_time
            while True:
                repeats += 1
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    time.sleep(delay)
                    delay = min(
                        start_sleep_time * factor ** repeats,
                        border_sleep_time)
                    cls_connection._connect()

        return inner

    return func_wrapper


def coroutine(func):
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn

    return inner
