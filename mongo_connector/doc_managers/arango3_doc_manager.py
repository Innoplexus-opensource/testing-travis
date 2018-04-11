"""
ArangoDB implementation of the DocManager interface.

Receives documents from an OplogThread and takes the appropriate actions on
ArangoDB.

Author: Prashant Patil

Email: prashant.patil@innoplexus.com

Date: 17/03/2018

Company: Innoplexus, Pune
"""

import os
import logging
import arango
from arango import ArangoClient
from arango.exceptions import DocumentInsertError, DatabaseCreateError, ServerConnectionError
from mongo_connector import errors, constants
from mongo_connector.util import exception_wrapper
from mongo_connector.doc_managers.doc_manager_base import DocManagerBase
from mongo_connector.command_helper import CommandHelper

wrap_exceptions = exception_wrapper({
    DatabaseCreateError: errors.OperationFailed,
    DocumentInsertError: errors.OperationFailed})

LOG = logging.getLogger(__name__)

__version__ = constants.__version__


__version__ = '0.1.4'
"""ArangoDB 3.X DocManager version."""


class DocManager(DocManagerBase):
    """ArangoDB implementation of the DocManager interface.

    Receives documents from an OplogThread and takes the appropriate actions on
    ArangoDB.
    """

    def __init__(self, url, auto_commit_interval=None,
                 chunk_size=constants.DEFAULT_MAX_BULK, **kwargs):
        """ Verify URL and establish a connection.
        """
        self.url = url
        self.auto_commit_interval = auto_commit_interval
        self.unique_key = '_key'
        self.chunk_size = chunk_size
        self.kwargs = kwargs

        self.arango = self.create_connection()

        # define meta database and meta collection names
        self.meta_database = "mongodb_meta"
        self.meta_collection = "mongodb_data"

        # check if meta database and meta collection are already present,
        # if not then create both
        if self.meta_database not in self.arango.databases():
            self.meta_database = self.arango.create_database(
                self.meta_database)
            self.meta_database.create_collection(self.meta_collection)
        else:
            self.meta_database = self.arango.db(self.meta_database)

        self.command_helper = CommandHelper()

    def verify_connection(self, connection):
        try:
            connection.verify()
        except ServerConnectionError:
            raise ServerConnectionError(
                "\nSeems that ArangoDB is running with Authentication.\n"
                "Please run the following,\n"
                "connector_arango_auth set, to set\n"
                "connector_arango_auth reset, to reset\n"
                "connector_arango_auth flush, to disable\n"
                "followed by source ~/.bashrc\n"
                "the ArangoDB authentication\n"
                "Refer readme.rst for more details")
        return

    @wrap_exceptions
    def create_connection(self):
        """Creates ArangoDB connection
        """
        # Extract host and port from URL
        host, port = self.get_host_port(self.url)
        # Extract Arango username and password from environment variable
        arango_username = os.environ.get('USER_ARANGO')
        arango_password = os.environ.get('PASSWD_ARANGO')

        if not arango_username and not arango_password:
            # Create Arrango connection
            arango_connection = ArangoClient(
                host=host, port=port)
            self.verify_connection(arango_connection)

        elif (not arango_username and arango_password) or \
                (arango_username and not arango_password):
            raise Exception("Invalid credentials, ArangoDB username/"
                            "password can't be blank")

        elif arango_username and arango_password:
            # Create Arrango connection
            arango_connection = ArangoClient(
                host=host, port=port, username=arango_username,
                password=arango_password)
            self.verify_connection(arango_connection)

        return arango_connection

    @wrap_exceptions
    def get_host_port(self, address):
        """Extracts host and port from URL
        """
        address_list = address.split(':')
        host = address_list[0]
        port = int(address_list[1])
        return host, port

    @wrap_exceptions
    def check_if_database_exists(self, database):
        """Checks if database exists
        """
        databases = self.arango.databases()
        if database in databases:
            return True

        return False

    @wrap_exceptions
    def check_if_collection_exists(self, database, coll):
        """Checks if collection exists
        """
        if not isinstance(database, arango.database.Database):
            database = self.arango.db(database)

        collections = database.collections()
        for item in collections:
            if item['name'] == coll:
                return True

        return False

    def apply_update(self, doc, update_spec):
        """Performs necessary update operations on the document and
        returns the updated document
        """
        if "$set" not in update_spec and "$unset" not in update_spec:
            # Don't try to add ns and _ts fields back in from doc
            return update_spec
        return super(DocManager, self).apply_update(doc, update_spec)

    def _db_and_collection(self, namespace):
        """Extracts the database and collection name
        from namespace string
        """
        return namespace.split('.', 1)

    def stop(self):
        """Stops any running threads
        """
        LOG.info(
            "Mongo DocManager Stopped: If you will not target this system "
            "again with mongo-connector then you may drop the database "
            "__mongo_connector, which holds metadata for Mongo Connector."
        )

    @wrap_exceptions
    def handle_command(self, doc, namespace, timestamp):
        """Handles operations at database as well as collection level
        like, create database, delete database, create collection,
        delete collection and rename collection
        """
        db, _ = self._db_and_collection(namespace)

        if doc.get('dropDatabase'):
            for new_db in self.command_helper.map_db(db):
                self.arango.delete_database(new_db)

        if doc.get('renameCollection'):
            source_namespace = self.command_helper.map_namespace(
                doc['renameCollection'])
            source_db, source_coll = self._db_and_collection(source_namespace)
            target_namespace = self.command_helper.map_namespace(doc['to'])
            target_db, target_coll = self._db_and_collection(target_namespace)

            if source_namespace and target_coll:
                source_db = self.arango.db(source_db)
                source_coll = source_db.collection(source_coll)
                source_coll.rename(
                    target_coll)

        if doc.get('create'):
            new_db, coll = self.command_helper.map_collection(
                db, doc['create'])
            if new_db:
                db_response = self.check_if_database_exists(new_db)
                if not db_response:
                    new_db = self.arango.create_database(db)

                coll_response = self.check_if_collection_exists(new_db, coll)
                if not coll_response:
                    if not isinstance(new_db, arango.database.Database):
                        new_db = self.arango.db(new_db)
                    new_db.create_collection(coll)

        if doc.get('drop'):
            new_db, coll = self.command_helper.map_collection(
                db, doc['drop'])
            new_db = self.arango.db(new_db)
            new_db.delete_collection(coll)

    @wrap_exceptions
    def update(self, document_id, update_spec, namespace, timestamp):
        """Apply updates given in update_spec to the document whose id
        matches that of doc.

        """
        document_id = self.pre_process_id(document_id)

        db, coll = self._db_and_collection(namespace)
        database = self.arango.db(db)
        coll = database.collection(coll)

        meta_collection = self.meta_database.collection(
            self.meta_collection)

        meta = {self.unique_key: document_id,
                "_ts": timestamp,
                "ns": namespace}

        meta_cursor = meta_collection.find(
            {self.unique_key: document_id,
             "ns": namespace})
        if meta_cursor.count():
            meta_collection.replace(meta)

        cursor = coll.find({self.unique_key: document_id})
        if cursor.count():
            document = cursor.next()
            updated = self.apply_update(document, update_spec)
            updated['_id'] = document_id
            self.upsert(updated, namespace, timestamp)
        else:
            LOG.error(
                "The document %s, which you are trying to update \
                is missing in ArangoDB"
                % document_id)

    @wrap_exceptions
    def upsert(self, doc, namespace, timestamp):
        """Update or insert a document into Mongo
        """
        # get database and collection name from namespace
        database, coll = self._db_and_collection(namespace)

        doc_ = {"create": coll}
        self.handle_command(doc_, namespace, timestamp)

        # get database instance
        database = self.arango.db(database)

        # get collection instance
        coll = database.collection(coll)

        # covert doc_id to string type from bson.objectid.ObjectId type
        doc_id = self.pre_process_id(doc.get('_id'))

        # pop _id from document
        doc.pop('_id')

        # get meta_collection instance
        meta_collection = self.meta_database.collection(
            self.meta_collection)

        # create meta for inserting into meta collection
        meta = {self.unique_key: doc_id,
                "_ts": timestamp,
                "ns": namespace}

        # check if the doc with given doc_id is already present in ArangoDB
        meta_cursor = meta_collection.find(
            {self.unique_key: doc_id,
             "ns": namespace})

        if meta_cursor.count():
            # replace existing doc
            meta_collection.replace(meta)
        else:
            # insert new doc
            meta_collection.insert(meta)

        # update "_id" field's value as "_key" field's value, as ArangoDB \
        # keeps "_key" as an unique key across the collection
        doc.update({self.unique_key: doc_id})

        if coll.has(doc_id):
            # replace existing doc
            coll.replace(doc)

        else:
            # insert new doc
            coll.insert(doc)

    def pre_process_id(self, doc_id):
        """Coverts doc id into string and ultimately into the format required
            for _key field
        """
        doc_id = str(doc_id)
        if '.' not in doc_id:
            doc_id = doc_id + '.'

        return doc_id

    @wrap_exceptions
    def bulk_upsert(self, docs, namespace, timestamp):
        """Performs bulk insert operations
        """
        dbname, collname = self._db_and_collection(namespace)

        doc = {"create": collname}
        self.handle_command(doc, namespace, timestamp)

        dbname = self.arango.db(dbname)
        collname = dbname.collection(collname)
        meta_coll = self.meta_database.collection(
            self.meta_collection)

        def iterate_chunks():

            more_chunks = True
            while more_chunks:
                bulk = []
                bulk_meta = []
                for i in range(self.chunk_size):
                    try:
                        doc = next(docs)
                        doc_id = doc.get('_id')
                        doc_id = self.pre_process_id(doc_id)
                        doc.pop('_id')
                        doc.update({self.unique_key: doc_id})
                        bulk.append(doc)
                        bulk_meta.append({
                            self.unique_key: doc_id,
                            'ns': namespace,
                            '_ts': timestamp
                        })
                    except StopIteration:
                        more_chunks = False
                        if i > 0:
                            yield bulk, bulk_meta, collname, meta_coll
                        break
                if more_chunks:
                    yield bulk, bulk_meta, collname, meta_coll

        for bulk_op, meta_bulk_op, collname, meta_coll in iterate_chunks():
            collname.import_bulk(documents=bulk_op, on_duplicate="replace")
            meta_coll.import_bulk(
                documents=meta_bulk_op,
                on_duplicate="replace")

    @wrap_exceptions
    def remove(self, document_id, namespace, timestamp):
        """Removes document from Mongo

        The input is a python dictionary that represents a mongo document.
        The documents has ns and _ts fields.
        """
        document_id = self.pre_process_id(document_id)

        database, coll = self._db_and_collection(namespace)
        database = self.arango.db(database)
        coll = database.collection(coll)

        meta_collection = self.meta_database.collection(
            self.meta_collection)

        meta_collection.delete(document_id)

        coll.delete(document_id)

    @wrap_exceptions
    def search(self, start_ts, end_ts):
        """Query ArangoDB for documents in a time range.

        This method is used to find documents that may be n conflict during
        a rollback event in MongoDB.
        """
        meta_coll = self.meta_database.collection(
            self.meta_collection)

        for doc in meta_coll.find_in_range("_ts", start_ts, end_ts):
            yield doc

    @wrap_exceptions
    def get_last_doc(self):
        """Return the document most recently modified in the target system
        """
        query = 'FOR doc IN {} SORT doc._ts DESC LIMIT 1 RETURN doc'.format(
            self.meta_collection)
        result = self.meta_database.aql.execute(query)
        for doc in result:
            return doc
