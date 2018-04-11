"""
Description: Test suit for testing the arango3-doc-manager

Author: Prashant Patil

Email: prashant.patil@innoplexus.com

Date: 17/03/2018

Company: Innoplexus, Pune
"""

from mongo_connector.doc_managers.arango3_doc_manager import DocManager
from mongo_connector.util import bson_ts_to_long
from bson import Timestamp
import time
import unittest


class TestArangodbMethods(unittest.TestCase):

    def setUp(self):
        """Initializes all required variables and set up arango connection, create test
            database and collection before every test method
        """
        self.url = "localhost:8529"
        self.timestamp = bson_ts_to_long(Timestamp(int(time.time()), 1))
        self.arango_doc_manager_obj = DocManager(self.url)
        self.arango_connection = self.arango_doc_manager_obj.arango

        self.database_name = "test_db"
        self.collection_name = "test_collection"
        self.namespace = self.database_name + '.' + self.collection_name

        self.db = self.arango_connection.create_database(self.database_name)
        self.collection = self.db.create_collection(self.collection_name)

    def tearDown(self):
        """Deletes test database and collection after every test method execution
        """
        self.arango_connection.delete_database(self.database_name)

    def remove_unnecessary_keys(self, doc):
        """Removes keys that we do not need while assertEqual from the document
        """
        if '_rev' in doc:
            doc.pop('_rev')
        if '_id' in doc:
            doc.pop('_id')

    def test_upsert(self):
        """Tests upsert method with test database
        """
        key = "111a"
        doc = {"name": "John", "_id": key}
        self.arango_doc_manager_obj.upsert(doc, self.namespace, self.timestamp)

        cursor = self.collection.find({"_key": key + '.'})
        for returned_doc in cursor:
            self.remove_unnecessary_keys(returned_doc)
            self.assertEqual(returned_doc, doc)

    def test_bulk_upsert(self):
        """Tests bulk upsert method with test database
        """
        docs = ({"_id": i} for i in range(1000))
        self.arango_doc_manager_obj.bulk_upsert(
            docs, self.namespace, self.timestamp)

        returned_count = self.collection.count()
        self.assertEqual(returned_count, 1000)

    def pre_process_id(self, doc_id):
        """Coverts doc id into string format and ultimately into the format required
            for _key field
        """
        doc_id = str(doc_id) + '.'
        return doc_id

    def fetch_document(self, key):
        """Fetches the document from arangodb satisfying a given condition
        """
        key = self.pre_process_id(key)

        cursor = self.collection.find({"_key": key})
        if cursor.count():
            for returned_doc in cursor:
                return returned_doc

        return None

    def test_update(self):
        """Tests update method with test database
        """
        key = "1"
        doc = {"name": "John", "_id": key}
        self.arango_doc_manager_obj.upsert(doc, self.namespace, self.timestamp)

        # Add new field as city
        spec = {"$set": {"city": "New York"}}
        self.arango_doc_manager_obj.update(
            key, spec, self.namespace, self.timestamp)

        returned_doc = self.fetch_document(key)
        self.remove_unnecessary_keys(returned_doc)

        if returned_doc:
            self.remove_unnecessary_keys(returned_doc)
            self.assertEqual(returned_doc, {"name": "John", "_key": key + '.',
                                            "city": "New York"})
        else:
            self.assertRaises("Document not found in target db")

        # Change city value
        spec = {"$set": {"city": "Chicago"}}
        self.arango_doc_manager_obj.update(
            key, spec, self.namespace, self.timestamp)
        returned_doc = self.fetch_document(key)
        if returned_doc:
            self.remove_unnecessary_keys(returned_doc)
            self.assertEqual(returned_doc, {"name": "John", "_key": key + '.',
                                            "city": "Chicago"})
        else:
            self.assertRaises("Document not found in target db")

        # Unset city_name field
        spec = {"$unset": {"city": 1}}
        self.arango_doc_manager_obj.update(
            key, spec, self.namespace, self.timestamp)
        returned_doc = self.fetch_document(key)
        if returned_doc:
            self.remove_unnecessary_keys(returned_doc)
            self.assertEqual(returned_doc, {"name": "John", "_key": key + '.'})

    def fetch_count(self):
        """Counts the number of records present in test database
        """
        count = self.collection.count()
        return count

    def test_remove(self):
        """Tests remove method with test database
        """
        key = "111a"
        doc = {"name": "John", "_id": key}
        self.arango_doc_manager_obj.upsert(doc, self.namespace, self.timestamp)

        self.arango_doc_manager_obj.remove(
            key + '.', self.namespace, self.timestamp)
        returned_count = self.fetch_count()

        self.assertEqual(returned_count, 0)

    def test_search(self):
        """Tests search method with test database
        """
        doc1 = {'_id': '1', 'name': 'John'}
        self.arango_doc_manager_obj.upsert(doc1, self.namespace, 1521056476)
        doc2 = {'_id': '2', 'name': 'Sam'}
        self.arango_doc_manager_obj.upsert(doc2, self.namespace, 1521142876)
        doc3 = {'_id': '3', 'name': 'Peter'}
        self.arango_doc_manager_obj.upsert(doc3, self.namespace, 1521229276)

        searched_documents = self.arango_doc_manager_obj.search(1521056476,
                                                                1521142876)

        searched_documents = [search for search in searched_documents]

        self.assertEqual(len(searched_documents), 2)
        result_ids = [result.get("_key") for result in searched_documents]
        self.assertIn('1.', result_ids)
        self.assertIn('2.', result_ids)

    def test_get_last_doc(self):
        """Tests get_last_doc method with test database
        """
        base = self.arango_doc_manager_obj.get_last_doc()
        ts = base.get("_ts", 0) if base else 0

        doc1 = {'_id': '1', 'name': 'John'}
        self.arango_doc_manager_obj.upsert(
            doc1, self.namespace, self.timestamp + 1)
        doc2 = {'_id': '2', 'name': 'Peter'}
        self.arango_doc_manager_obj.upsert(
            doc2, self.namespace, self.timestamp + 2)
        doc3 = {'_id': '3', 'name': 'Mark'}
        self.arango_doc_manager_obj.upsert(
            doc3, self.namespace, self.timestamp + 3)

        count = self.fetch_count()

        self.assertEqual(count, 3)

        doc = self.arango_doc_manager_obj.get_last_doc()

        self.assertEqual(doc['_key'], '3.')

        doc4 = {'_id': '3', 'name': 'Sam'}
        self.arango_doc_manager_obj.upsert(
            doc4, self.namespace, self.timestamp + 4)

        doc = self.arango_doc_manager_obj.get_last_doc()

        self.assertEqual(doc['_key'], '3.')

        self.assertEqual(count, 3)

if __name__ == '__main__':
    unittest.main()
