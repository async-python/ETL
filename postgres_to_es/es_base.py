import json
from dataclasses import asdict
from typing import Optional

from elasticsearch import Elasticsearch
from elasticsearch import TransportError

from etl_decorators import backoff
from etl_settings import EtlConfig, logger


class EsBase:
    def __init__(self):
        cnf = EtlConfig()
        self.host = cnf.elastic_host
        self.port = cnf.elastic_port
        self.scheme = cnf.elastic_scheme
        self.http_auth = (cnf.elastic_user, cnf.elastic_password)
        self.index_name = cnf.elastic_index

        self.es = self.connect()

    def connect(self) -> Elasticsearch:
        return Elasticsearch(self.host, port=self.port, scheme=self.scheme,
                             http_auth=self.http_auth)

    @backoff()
    def create_index(self, index_name='', index_body=''):
        try:
            self.es.indices.create(index_name, body=index_body)
        except TransportError as e:
            logger.warning(e)

    @backoff()
    def bulk_update(self, docs) -> Optional[bool]:
        if not docs:
            logger.warning('No more data to update in elastic')
            return None
        body = ''
        for doc in docs:
            index = {'index': {'_index': self.index_name, '_id': doc.id}}
            body += json.dumps(index) + '\n' + json.dumps(asdict(doc)) + '\n'

        results = self.es.bulk(body=body)
        if results['errors']:
            error = [result['index'] for result in results['items'] if
                     result['index']['status'] != 200]
            logger.debug(results['took'])
            logger.debug(results['errors'])
            logger.debug(error)
            return None
        return True
