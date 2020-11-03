# -*- coding: utf-8 -*-

from elasticsearch.exceptions import NotFoundError

from tests import TestElasticmock, INDEX_NAME, DOC_TYPE, BODY


class TestUpdate(TestElasticmock):

    def test_should_raise_notfounderror_when_nonindexed_id_is_used_for_update(self):
        with self.assertRaises(NotFoundError):
            self.es.update(id="1", body={"doc": BODY}, index=INDEX_NAME)

    def test_should_raise_notfounderror_when_nonindexed_id_is_used_for_update_if_ignored(self):
        with self.assertRaises(NotFoundError):
            self.es.update(id="1", body={"doc": BODY}, index=INDEX_NAME, ignore=404)

    def test_should_update_document_new_path(self):
        doc = self.es.index(index=INDEX_NAME, doc_type=DOC_TYPE, body=BODY)

        update_data = {"doc": {"updated_by": "joosterman"}}
        updated_doc = self.es.update(id=doc["_id"], index=INDEX_NAME, doc_type=DOC_TYPE, body=update_data, _source=True)

        expected = {
            "_type": DOC_TYPE,
            "_source": {**BODY, "updated_by": "joosterman"},
            "_index": INDEX_NAME,
            "_version": doc["_version"] + 1,
            "_id": doc["_id"]
        }

        self.assertDictEqual(expected, updated_doc)

    def test_should_update_document_overwrite_path(self):
        doc = self.es.index(index=INDEX_NAME, doc_type=DOC_TYPE, body={'author': 'kimchi'})

        update_data = {"doc": {"author": "vrcmarcos"}}
        updated_doc = self.es.update(id=doc["_id"], index=INDEX_NAME, doc_type=DOC_TYPE, body=update_data, _source=True)

        expected = {
            "_type": DOC_TYPE,
            "_source": {"author": "vrcmarcos"},
            "_index": INDEX_NAME,
            "_version": doc["_version"] + 1,
            "_id": doc["_id"]
        }

        self.assertDictEqual(expected, updated_doc)
