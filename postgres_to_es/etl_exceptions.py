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
