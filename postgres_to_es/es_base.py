import json

from elasticsearch import Elasticsearch
from etl_decorators import backoff
from etl_settings import EtlConfig, logger

SUCCESSFUL_STATUS_CODES = [200, 201, 202, 203, 204, 205, 206, 207, 208, 226]


class EsBase:
    """Класс - адаптер Elasticsearch для ETL процесса"""
    def __init__(self):
        conf = EtlConfig()
        self.host = conf.elastic_host
        self.port = conf.elastic_port
        self.scheme = conf.elastic_scheme
        self.http_auth = (conf.elastic_user, conf.elastic_password)
        self.index_name = conf.elastic_index
        self.es = self.connect()
        with open('es_schema.json') as file:
            settings = json.load(file)
            self.create_index(self.index_name, json.dumps(settings))

    def connect(self) -> Elasticsearch:
        return Elasticsearch(hosts=self.host, port=self.port,
                             scheme=self.scheme, http_auth=self.http_auth)

    @backoff()
    def create_index(self, index_name, index_body) -> None:
        """
        Создание индекса,
        в случае если индекс существует вывод предупреждения
        """
        if self.es.indices.exists(index=index_name):
            logger.warning(f'Индекс {index_name} уже существует')
        else:
            self.es.indices.create(index=index_name, body=index_body)

    @backoff()
    def bulk_create(self, index_body: str) -> None:
        """Пакетная загрузка данных в индекс"""
        results = self.es.bulk(body=index_body, doc_type='application/json')
        if results['errors']:
            error = [result['index'] for result in results['items'] if
                     result['index']['status'] not in SUCCESSFUL_STATUS_CODES]
            logger.warning(results['took'])
            logger.warning(results['errors'])
            logger.warning(error)
