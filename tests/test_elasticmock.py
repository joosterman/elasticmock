# -*- coding: utf-8 -*-

from unittest import TestCase

import elasticsearch
from elasticsearch.exceptions import NotFoundError

from elasticmock import elasticmock
from elasticmock.fake_elasticsearch import FakeElasticsearch


class TestFakeElasticsearch(TestCase):

    @elasticmock
    def setUp(self):
        self.es = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
        self.index_name = 'test_index'
        self.doc_type = 'doc-Type'
        self.body = {'string': 'content', 'id': 1}

    def test_should_create_fake_elasticsearch_instance(self):
        self.assertIsInstance(self.es, FakeElasticsearch)

    def test_should_index_document(self):
        data = self.es.index(index=self.index_name, doc_type=self.doc_type, body=self.body)

        self.assertEqual(self.doc_type, data.get('_type'))
        self.assertTrue(data.get('created'))
        self.assertEqual(1, data.get('_version'))
        self.assertEqual(self.index_name, data.get('_index'))

    def test_should_raise_notfounderror_when_nonindexed_id_is_used(self):
        self.index_name = 'test_index'

        with self.assertRaises(NotFoundError):
            self.es.get(index=self.index_name, id='1')

    def test_should_get_document_with_id(self):
        data = self.es.index(index=self.index_name, doc_type=self.doc_type, body=self.body)

        document_id = data.get('_id')
        target_doc = self.es.get(index=self.index_name, id=document_id)

        expected = {
            '_type': self.doc_type,
            '_source': self.body,
            '_index': self.index_name,
            '_version': 1,
            'found': True,
            '_id': document_id
        }

        self.assertDictEqual(expected, target_doc)

    def test_should_get_document_with_id_and_doc_type(self):
        data = self.es.index(index=self.index_name, doc_type=self.doc_type, body=self.body)

        document_id = data.get('_id')
        target_doc = self.es.get(index=self.index_name, id=document_id, doc_type=self.doc_type)

        expected = {
            '_type': self.doc_type,
            '_source': self.body,
            '_index': self.index_name,
            '_version': 1,
            'found': True,
            '_id': document_id
        }

        self.assertDictEqual(expected, target_doc)

    def test_should_return_exists_false_if_nonindexed_id_is_used(self):
        self.assertFalse(self.es.exists(index=self.index_name, doc_type=self.doc_type, id=1))

    def test_should_return_exists_true_if_indexed_id_is_used(self):
        data = self.es.index(index=self.index_name, doc_type=self.doc_type, body=self.body)
        document_id = data.get('_id')
        self.assertTrue(self.es.exists(index=self.index_name, doc_type=self.doc_type, id=document_id))

    def test_should_return_true_when_ping(self):
        self.assertTrue(self.es.ping())

    def test_should_return_status_200_for_info(self):
        info = self.es.info()
        self.assertEqual(info.get('status'), 200)

    def test_should_get_only_document_source_with_id(self):
        data = self.es.index(index=self.index_name, doc_type=self.doc_type, body=self.body)

        document_id = data.get('_id')
        target_doc_source = self.es.get_source(index=self.index_name, doc_type=self.doc_type, id=document_id)

        self.assertEqual(target_doc_source, self.body)

    def test_should_raise_notfounderror_when_search_for_unexistent_index(self):
        self.index_name = 'test_index'

        with self.assertRaises(NotFoundError):
            self.es.search(index=self.index_name)

    def test_should_return_all_documents(self):
        index_quantity = 10
        for i in range(0, index_quantity):
            self.es.index(index='index_{0}'.format(i), doc_type=self.doc_type, body={'data': 'test_{0}'.format(i)})

        search = self.es.search()
        self.assertEqual(index_quantity, search.get('hits').get('total'))

    def test_should_return_only_indexed_documents_on_index(self):
        index_quantity = 2
        for i in range(0, index_quantity):
            self.es.index(index=self.index_name, doc_type=self.doc_type, body={'data': 'test_{0}'.format(i)})

        search = self.es.search(index=self.index_name)
        self.assertEqual(index_quantity, search.get('hits').get('total'))

    def test_should_return_only_indexed_documents_on_index_with_doc_type(self):
        index_quantity = 2
        for i in range(0, index_quantity):
            self.es.index(index=self.index_name, doc_type=self.doc_type, body={'data': 'test_{0}'.format(i)})
        self.es.index(index=self.index_name, doc_type='another-Doctype', body={'data': 'test'})

        search = self.es.search(index=self.index_name, doc_type=self.doc_type)
        self.assertEqual(index_quantity, search.get('hits').get('total'))

    @elasticmock
    def test_should_return_same_elastic_instance_when_instantiate_more_than_one_instance_with_same_host(self):
        es1 = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
        es2 = elasticsearch.Elasticsearch(hosts=[{'host': 'localhost', 'port': 9200}])
        self.assertEqual(es1, es2)
