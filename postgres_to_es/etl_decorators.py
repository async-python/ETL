import logging
import sys
import time
from functools import wraps

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.DEBUG)
formatter = logging.Formatter(
    '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)


def backoff(start_sleep_time=0.5, factor=2, border_sleep_time=30):
    """
    Функция для повторного выполнения функции через некоторое время,
    если возникла ошибка. Использует наивный экспоненциальный рост времени
    повтора (factor) до граничного времени ожидания (border_sleep_time)
    Формула:
        t = start_sleep_time * 2^(n) if t < border_sleep_time
        t = border_sleep_time if t >= border_sleep_time
    :param start_sleep_time: начальное время повтора
    :param factor: во сколько раз нужно увеличить время ожидания
    :param border_sleep_time: граничное время ожидания
    :return: результат выполнения функции
    """

    def func_wrapper(func):
        @wraps(func)
        def inner(*args, **kwargs):
            repeats, delay = 0, 0
            while True:
                repeats += 1
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if delay >= border_sleep_time:
                        delay = border_sleep_time
                    else:
                        delay = min(start_sleep_time * factor ** repeats,
                                    border_sleep_time)
                    logger.info(e)
                    logger.info(f'следующая попытка через {delay}')
                    time.sleep(delay)

        return inner

    return func_wrapper


def coroutine(func):
    """Активация корутины"""
    @wraps(func)
    def inner(*args, **kwargs):
        fn = func(*args, **kwargs)
        next(fn)
        return fn

    return inner
