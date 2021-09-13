from datetime import datetime, timedelta
from typing import List, Optional

from etl_dataclasses import PgFilmID, PgFilmWork, PgRowsCount
from etl_decorators import backoff
from etl_settings import EtlConfig, logger
from psycopg2 import connect as pg_conn
from psycopg2.extras import DictCursor


class PgBase:
    """Класс - адаптер PostgreSQL для ETL процесса"""

    BEGIN = ('SELECT id, updated_at FROM film_work ORDER BY '
             'updated_at, id LIMIT 1')
    TOTAL = 'SELECT COUNT(*) FROM film_work '
    COUNT = TOTAL + 'WHERE updated_at > %s'
    GET_FILM_IDS = '''SELECT id, updated_at FROM film_work WHERE 
        updated_at  > %s ORDER BY updated_at, id LIMIT %s'''
    GET_FILMS_BY_ID = '''
        SELECT
            fw.id, fw.title, fw.description, fw.type,
            ARRAY_AGG(DISTINCT genre.name ) AS genres,
            fw.rating, fw.creation_date, fw.certificate, fw.age_limit, 
            fw.file_path, 
            ARRAY_AGG(DISTINCT CONCAT(person.first_name,' ',person.middle_name
            ,' ',person.last_name)) FILTER (WHERE person_fw.role = 'director')
            AS directors,
            ARRAY_AGG(DISTINCT CONCAT(person.first_name,' ',person.middle_name
            ,' ',person.last_name)) FILTER (WHERE person_fw.role = 'actor') 
            AS actors,
            ARRAY_AGG(DISTINCT CONCAT(person.first_name,' ',person.middle_name
            ,' ',person.last_name)) FILTER (WHERE person_fw.role = 'writer') 
            AS writers
        FROM film_work AS fw
        LEFT OUTER JOIN person_film_work AS person_fw ON 
            fw.id = person_fw.film_work_id
        LEFT OUTER JOIN person ON person_fw.person_id = person.id
        LEFT OUTER JOIN genre_film_work AS genre_fw ON 
            fw.id = genre_fw.film_work_id
        LEFT OUTER JOIN genre ON genre_fw.genre_id = genre.id
        WHERE fw.id IN %s
        GROUP BY fw.id
    '''

    def __init__(self):
        self.cnf = EtlConfig()
        self.conn = self.connect()

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
    def query_list_rows(self, sql_query: str, query_args: tuple) -> dict:
        """Запрос списка строк в БД"""
        self.conn = self.connect() if self.conn.closed != 0 else self.conn
        with self.conn as conn, conn.cursor() as cur:
            cur.execute(sql_query, query_args)
            rows = cur.fetchall()
        return rows

    def verify_data_exists(self) -> bool:
        """Проверка на наличие данных в таблице"""
        row = self.query_one_row(self.TOTAL, None)
        if PgRowsCount(**row).count:
            return True
        return False

    def get_rows_count(self, date: datetime) -> int:
        """Получаем общее число строк к записи в ES"""
        row = self.query_one_row(self.COUNT, (date,))
        return PgRowsCount(**row).count

    def get_first_film_update_time(self) -> Optional[datetime]:
        """Возвращает дату первого обновления фильма по полю updated_at"""
        row = self.query_one_row(self.BEGIN, None)
        try:
            time = PgFilmID(**row)
            return time.updated_at - timedelta(0, 0, 0, 1)
        except Exception as e:
            logger.warning(e)
            return None

    def get_films_ids(
            self, last_time: datetime, limit: int) -> List[PgFilmID]:
        """
        Возвращает список ID фильмов, сортированных по возрастанию даты
        обновления (поля updated_at).
        :param last_time: дата поля update_at в выборке будет выше входящего
        значения параметра last_time.
        :param limit: число ID в выборке.
        """
        id_list = [PgFilmID(**row) for row in self.query_list_rows(
            self.GET_FILM_IDS, (last_time, limit,))]
        return id_list

    def get_films_by_id(self, id_list: tuple) -> List[PgFilmWork]:
        """
        Получает данные из БД и возвращает преобразованными в List[dataclass]
        :param: id_list: список ID кинопроизведений для запроса
        """
        films = [PgFilmWork(**row) for row in
                 self.query_list_rows(self.GET_FILMS_BY_ID, (id_list,))]
        return films
