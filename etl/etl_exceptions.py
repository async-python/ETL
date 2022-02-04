class ExceededConnectLimitException(Exception):
    def __init__(self, func):
        self.func_name = func.__name__
        super().__init__(
            f'Не удалось выполнить функцию {self.func_name} - превышен '
            f'лимит попыток соединения.'
        )


class EmptyStartTimeException(Exception):
    def __init__(self):
        super().__init__(
            'Не удалось получить значение поля update_at в таблице film_work'
        )


class ZeroPgRowsException(Exception):
    def __init__(self, table, time):
        super().__init__(
            f'В таблице {table} количество строк для поля '
            f'updated_at > {time} равно 0'
        )


class DataDoesNotExistException(Exception):
    def __init__(self):
        super().__init__(
            'В таблице данные отсутствуют'
        )


class ProcessAlreadyExistException(Exception):
    def __init__(self):
        super().__init__(
            'Процесс Etl уже запущен'
        )
