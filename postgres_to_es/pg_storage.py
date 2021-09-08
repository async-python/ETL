from psycopg2 import connect as pg_conn, sql
from decorators import backoff
from settings import EtlConfig


class PgStorage:
    FIRSTTIME = 'SELECT updated_at FROM {} ORDER BY updated_at LIMIT 1'
    UPDATED = 'SELECT id, updated_at FROM {} WHERE updated_at  > %s ORDER BY modified LIMIT %s'
    GETFILMSBYID = '''
    SELECT
        fw.id, fw.rating, fw.imdb_tconst, ft.name,
        ARRAY_AGG(DISTINCT fg.name ) AS genres,
        fw.title, fw.description,
        ARRAY_AGG(DISTINCT fp.id || ' : ' || fp.full_name) FILTER (WHERE fwp.role = 'director') AS directors,
        ARRAY_AGG(DISTINCT fp.id || ' : ' || fp.full_name) FILTER (WHERE fwp.role = 'actor') AS actors,
        ARRAY_AGG(DISTINCT fp.id || ' : ' || fp.full_name) FILTER (WHERE fwp.role = 'writer') AS writers,
        fw.updated_at
    FROM filmwork AS fw
    LEFT OUTER JOIN filmworkperson AS fwp ON fw.id = fwp.film_work_id
    LEFT OUTER JOIN filmperson AS fp ON fwp.person_id = fp.id
    LEFT OUTER JOIN filmworkgenre AS fwg ON fw.id = fwg.film_work_id
    LEFT OUTER JOIN filmgenre AS fg ON fwg.genre_id = fg.id
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
