from couchbase.options import ClusterOptions  # Updated import
from couchbase.cluster import Cluster
from couchbase.auth import PasswordAuthenticator
from couchbase.exceptions import CouchbaseException, DocumentExistsException  # Import the exception
from couchbase.collection import UpsertOptions

class CouchbaseClient:
    def __init__(self, connection_string, username, password, bucket_name):
        """
        Initialize the Couchbase client.
        """
        try:
            self.cluster = Cluster(connection_string, ClusterOptions(PasswordAuthenticator(username, password)))
            self.bucket = self.cluster.bucket(bucket_name)
            self.collection = self.bucket.default_collection()
            self.bucket_name = bucket_name
        except CouchbaseException as e:
            print(f"Error connecting to Couchbase: {e}")
            raise
        except Exception as e:
            print(f"An error occurred: {e}")
            raise

    def get_document_by_key(self, key):
        """
        Retrieve a document by its key.
        """
        try:
            result = self.collection.get(key)
            return result.content_as[dict]
        except CouchbaseException as e:
            print(f"Error retrieving document with key {key}: {e}")
            return None

    def insert_document(self, key, document):
        """
        Insert a new document with the given key.
        Throws an error if the key already exists.
        """
        try:
            self.collection.insert(key, document)  # Insert will fail if the key exists
            print(f"Document with key {key} inserted successfully.")
            return True
        except DocumentExistsException:
            print(f"Error: Document with key {key} already exists.")
            return False
        except CouchbaseException as e:
            print(f"Error inserting document with key {key}: {e}")
            return False

    def update_document_by_key(self, key, document):
        """
        Update an existing document by its key.
        """
        try:
            self.collection.upsert(key, document, UpsertOptions())
            print(f"Document with key {key} updated successfully.")
        except CouchbaseException as e:
            print(f"Error updating document with key {key}: {e}")

    def delete_document_by_key(self, key):
        """
        Delete a document by its key.
        """
        try:
            self.collection.remove(key)
            print(f"Document with key {key} deleted successfully.")
        except CouchbaseException as e:
            print(f"Error deleting document with key {key}: {e}")

    def get_all_products_by_status(self, status):
        """
        Query the database for all products with the given status.
        """
        try:
            query = f"SELECT * FROM {self.bucket_name} WHERE status = $1"
            result = self.cluster.query(query, status)
            return result.rows()
        except CouchbaseException as e:
            print(f"Error querying products by status: {e}")
            return []