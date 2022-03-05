import json
from dataclasses import asdict

from elasticsearch import Elasticsearch

from etl_decorators import backoff
from etl_settings import EtlConfig, logger

SUCCESSFUL_STATUS_CODES = [200, 201, 202, 203, 204, 205, 206, 207, 208, 226]


class EsAdapter:
    """Класс - адаптер Elasticsearch для Etl процесса"""

    def __init__(self, index: str):
        conf = EtlConfig()
        self.index_name = index
        self.host = conf.elastic_host
        self.port = conf.elastic_port
        self.scheme = conf.elastic_scheme
        self.es = self.connect()
        self.test_mode = conf.etl_test_mode

    def connect(self) -> Elasticsearch:
        return Elasticsearch(hosts=self.host, port=self.port,
                             scheme=self.scheme)

    @backoff()
    def bulk_create(self, data) -> None:
        """Пакетная загрузка данных в индекс"""
        index_body = ''
        for item in data:
            index = {'index': {'_index': self.index_name, '_id': item.id}}
            index_body += json.dumps(index) + '\n' + json.dumps(
                asdict(item)) + '\n'
        if self.test_mode:
            with open(f'{self.index_name}.json', 'rw') as f:
                exist_data = json.load(f)
                exist_data += index_body
                json.dump(exist_data, f)
        else:
            results = self.es.bulk(body=index_body,
                                   doc_type='application/json')
            if results['errors']:
                error = [result['index'] for result in results['items'] if
                         result['index'][
                             'status'] not in SUCCESSFUL_STATUS_CODES]
                logger.warning(results['took'])
                logger.warning(results['errors'])
                logger.warning(error)
