# Проектное задание: ETL

ETL процесс для передачи данных из PostgreSQL в Elasticsearch.

## Стек технологий

Docker, Redis, Postgres, Elasticsearch

## Запуск проекта

1. Запуск postgres: `docker run -d --rm --name postgres -p 5432:5432 -v /path/to/my/folder:/var/lib/postgresql/data -e POSTGRES_PASSWORD="<password>" postgres:13`
   (Заполнить БД тестовыми данными скриптом 1-го спринта)
2. Запуск redis: `docker run --rm --name some-redis -p 6379:6379 -v /path/to/my/folder:/data -d redis redis-server requirepass "<password>"`
3. Запуск elasticsearch: `docker run --rm -d --name elasticsearch -p 9200:9200 -e "discovery.type=single-node" -e "xpack.security.enabled=true" -e "ELASTIC_USER=<username>" -e "ELASTIC_PASSWORD=<password>" elasticsearch:7.7.0`
4. Установка связей: `pip install -r requirements.txt`
5. Создать в корне проекта .env файл с переменными:
- POSTGRES_DB=movies
- POSTGRES_HOST=127.0.0.1
- POSTGRES_USER=postgres
- POSTGRES_PORT=5432
- POSTGRES_PASSWORD=<password>
- POSTGRES_OPTIONS=-c search_path=content
- REDIS_HOST=127.0.0.1
- REDIS_PORT=6379
- REDIS_PASSWORD=<password>
- ELASTIC_HOST=127.0.0.1
- ELASTIC_PORT=9200
- ELASTIC_USER=elastic
- ELASTIC_PASSWORD=<password>
- ELASTIC_INDEX=movies
- ETL_BUTCH_SIZE=10000
- ETL_LOG_LEVEL=DEBUG
6. Запуск ETL процесса - etl_start.py
7. Остановка ETL процесса - etl_stop.py
