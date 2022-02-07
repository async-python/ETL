from datetime import datetime, timedelta
from typing import Any, List, Optional

from psycopg2 import connect as pg_conn
from psycopg2.extras import DictCursor

from etl_dataclasses import PgFilmWork, PgGenre, PgObjID, PgPerson, PgRowsCount
from etl_decorators import backoff
from etl_exceptions import DataDoesNotExistException, ZeroPgRowsException
from etl_settings import EtlConfig, logger

FIRST_ROW_QUERY = '''SELECT id, updated_at FROM {table} ORDER BY 
             updated_at, id LIMIT 1'''
TOTAL_ROWS_QUERY = 'SELECT COUNT(*) FROM {table} '
SELECT_IDS = '''SELECT id, updated_at FROM {table} WHERE 
    updated_at  > %s ORDER BY updated_at, id LIMIT %s'''
SELECT_DATA_BY_IDS = {
    'film_work': '''
    SELECT
        fw.id, fw.title, fw.description, fw.type,
        ARRAY_AGG(DISTINCT jsonb_build_object(
        'id', genre.id, 'name', genre.name )) AS genres,
        fw.rating, fw.creation_date, fw.certificate, fw.age_limit, 
        fw.file_path, 
        ARRAY_AGG(DISTINCT jsonb_build_object('id', person.id, 'name', 
        CONCAT(person.first_name,' ',person.middle_name
        ,' ',person.last_name))) FILTER (WHERE person_fw.role = 'director')
        AS directors,
        ARRAY_AGG(DISTINCT jsonb_build_object('id', person.id, 'name', 
        CONCAT(person.first_name,' ',person.middle_name
        ,' ',person.last_name))) FILTER (WHERE person_fw.role = 'actor')
        AS actors,
        ARRAY_AGG(DISTINCT jsonb_build_object('id', person.id, 'name', 
        CONCAT(person.first_name,' ',person.middle_name
        ,' ',person.last_name))) FILTER (WHERE person_fw.role = 'writer')
        AS writers
    FROM film_work AS fw
    LEFT OUTER JOIN person_film_work AS person_fw ON 
        fw.id = person_fw.film_work_id
    LEFT OUTER JOIN person ON person_fw.person_id = person.id
    LEFT OUTER JOIN genre_film_work AS genre_fw ON 
        fw.id = genre_fw.film_work_id
    LEFT OUTER JOIN genre ON genre_fw.genre_id = genre.id
    WHERE fw.id IN %s
    GROUP BY fw.id''',
    'genre': 'SELECT id, name, description FROM genre WHERE genre.id IN %s',
    'person': '''SELECT person.id, person.birth_date, 
    CONCAT(person.first_name,' ',person.middle_name
        ,' ',person.last_name) as full_name,
    ARRAY_AGG(DISTINCT person_fw.role) as role,
    ARRAY_AGG(DISTINCT person_fw.film_work_id) as films
    FROM person 
    LEFT OUTER JOIN person_film_work AS person_fw ON 
    person.id = person_fw.person_id
    WHERE person.id IN %s
    GROUP BY person.id
    '''
}
DATACLASSES = {
    'film_work': PgFilmWork,
    'genre': PgGenre,
    'person': PgPerson,
}


class PgAdapter:
    """Класс - адаптер PostgresSQL для Etl процесса"""

    def __init__(self, table_name: str):
        self.table_name = table_name
        self.cnf = EtlConfig()
        self.conn = self.connect()
        self._init_queries()
        self.dataclass: type = DATACLASSES.get(self.table_name)
        self._verify_data_exists()

    def _init_queries(self):
        """Инициализация SQL запросов согласно выбранной таблице в БД"""
        self.first_row: str = FIRST_ROW_QUERY.format(table=self.table_name)
        self.total_count: str = TOTAL_ROWS_QUERY.format(table=self.table_name)
        self.current_count: str = self.total_count + 'WHERE updated_at > %s'
        self.select_ids: str = SELECT_IDS.format(table=self.table_name)
        self.select_rows_by_ids: str = SELECT_DATA_BY_IDS.get(self.table_name)

    def _verify_data_exists(self) -> None:
        """Проверка на наличие данных в таблице"""
        row = self.query_one_row(self.total_count, None)
        if PgRowsCount(**row).count == 0:
            raise DataDoesNotExistException

    @backoff()
    def connect(self):
        return pg_conn(
            dbname=self.cnf.postgres_db,
            user=self.cnf.postgres_user,
            password=self.cnf.postgres_password,
            host=self.cnf.postgres_host,
            port=self.cnf.postgres_port,
            options=self.cnf.postgres_options,
            cursor_factory=DictCursor
        )

    @backoff()
    def query_one_row(self, sql_query: str, query_args: Optional) -> dict:
        """Запрос одной строки в БД"""
        self.conn = self.connect() if self.conn.closed != 0 else self.conn
        with self.conn as conn, conn.cursor() as cur:
            cur.execute(sql_query, query_args)
            row = cur.fetchone()
        return row

    @backoff()
    def query_list_rows(self, sql_query: str,
                        query_args: tuple) -> list[Any]:
        """Запрос списка строк в БД"""
        self.conn = self.connect() if self.conn.closed != 0 else self.conn
        with self.conn as conn, conn.cursor() as cur:
            cur.execute(sql_query, query_args)
            rows = cur.fetchall()
        return rows

    def get_rows_count(self, date: datetime) -> int:
        """Получаем общее число строк к записи в ES"""
        row = self.query_one_row(self.current_count, (date,))
        count = PgRowsCount(**row).count
        if count == 0:
            raise ZeroPgRowsException(self.table_name, date)
        return count

    def get_first_update_time(self) -> Optional[datetime]:
        """Возвращает дату первого обновления в таблице по полю updated_at"""
        row = self.query_one_row(self.first_row, None)
        try:
            time = PgObjID(**row)
            return time.updated_at - timedelta(0, 0, 0, 1)
        except Exception as error:
            logger.error(error)
            return None

    def get_data_ids(
            self, last_time: datetime, limit: int) -> List[PgObjID]:
        """
        Возвращает список ID, сортированных по возрастанию даты
        обновления (поля updated_at).
        :param last_time: дата поля update_at в выборке будет выше входящего
        значения параметра last_time.
        :param limit: число ID в выборке.
        """
        id_list = [PgObjID(**row) for row in self.query_list_rows(
            self.select_ids, (last_time, limit,))]
        return id_list

    def get_data_by_ids(self, id_list: tuple) -> List[type]:
        """
        Получает данные из БД и возвращает преобразованными в List[dataclass]
        :param: id_list: список ID для запроса
        """
        data = [self.dataclass(**row) for row in
                self.query_list_rows(self.select_rows_by_ids, (id_list,))]
        return data
