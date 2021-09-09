from psycopg2 import connect as pg_conn, sql
from decorators import backoff
from settings import EtlConfig


class PgBase:
    BEGIN = 'SELECT updated_at FROM film_work ORDER BY updated_at LIMIT 1'
    GET_FILM_IDS = 'SELECT id, updated_at FROM film_work WHERE updated_at  > %s ORDER BY updated_at LIMIT %s'
    GET_FILMS_BY_ID = '''
    SELECT
        fw.id, fw.rating, fw.description, fw.type, fw.certificate,
        ARRAY_AGG(DISTINCT genre.name ) AS genres,
        fw.title, fw.age_limit, fw.file_path, fw.created_at,
        ARRAY_AGG(STRUCT(person.first_name, person.middle_name, person.last_name)) FILTER (WHERE person_fw.role = 'director') AS directors,
        ARRAY_AGG(STRUCT(person.first_name, person.middle_name, person.last_name)) FILTER (WHERE person_fw.role = 'actor') AS actors,
        ARRAY_AGG(STRUCT(person.first_name, person.middle_name, person.last_name)) FILTER (WHERE person_fw.role = 'writer') AS writers,
        fw.updated_at
    FROM film_work AS fw
    LEFT OUTER JOIN person_film_work AS person_fw ON fw.id = person_fw.film_work_id
    LEFT OUTER JOIN person as person ON person_fw.person_id = person.id
    LEFT OUTER JOIN genre_film_work AS genre_fw ON fw.id = genre_fw.film_work_id
    LEFT OUTER JOIN genre AS genre ON genre_fw.genre_id = genre.id
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
        )
