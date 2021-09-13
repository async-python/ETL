# Проектное задание: ETL

ETL процесс для передачи данных из PostgreSQL в Elasticsearch.

## Стек технологий

Docker, Redis, Postgres, Elasticsearch

## Запуск проекта

1. Запуск postgres: `docker run -d --rm --name postgres -p 5432:5432 -v /path/to/my/folder:/var/lib/postgresql/data -e POSTGRES_PASSWORD="<password>" postgres:13`
2. Запуск redis: `docker run --rm --name some-redis -p 6379:6379 -v /path/to/my/folder:/data -d redis redis-server requirepass "<password>"`
3. Запуск elasticsearch: `docker run --rm -d --name elasticsearch -p 9200:9200 -e "discovery.type=single-node" -e "xpack.security.enabled=true" -e "ELASTIC_USER=<username>" -e "ELASTIC_PASSWORD=<password>" elasticsearch:7.7.0`
4. Установка связей: `pip install -r requirements.txt`
5. Запуск ETL процесса - etl_start.py
6. Остановка ETL процесса - etl_stop.py
