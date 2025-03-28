from flask import Flask, request, jsonify
from dotenv import load_dotenv
import json
from datetime import datetime
import os
from src.model import Product  # Import the Product class
from src.couchbase_client import CouchbaseClient  # Import the CouchbaseClient class


load_dotenv(dotenv_path="config/.env")

app = Flask(__name__)

# Access environment variables
connection_string = os.getenv("CONNECTION_STRING")
username = os.getenv("USERNAME")
password = os.getenv("PASSWORD")
bucket_name = os.getenv("BUCKETNAME")

# Initialize Couchbase client
couchbase_client = CouchbaseClient(
    connection_string=connection_string,
    username=username,
    password=password,
    bucket_name=bucket_name
)

@app.route('/products/getAllProducts', methods=['GET'])
def get_all_products():
    status = request.args.get('status', 'ACTIVE')  # Default value is 'ACTIVE'
    # Query the database for products with the given status
    # This is a placeholder; implement the actual query logic in CouchbaseClient
    products = couchbase_client.get_all_products_by_status(status)

    return jsonify(list(products))

@app.route('/products/<id>', methods=['GET'])
def get_product_by_id(id):
    # Retrieve the product by ID
    product = couchbase_client.get_document_by_key(id)
    if product:
        return jsonify(product)
    return jsonify({"error": "Product not found"}), 404

@app.route('/products', methods=['POST'])
def create_product():
    product_data = request.json
    product_data['created_at'] = datetime.now().isoformat()
    #map the product data to the Product class
    product = Product(**product_data)
    if not product_data or 'id' not in product_data:
        return jsonify({"error": "Invalid product data"}), 400
    response = couchbase_client.insert_document(product_data['id'], product_data)
    if not response:
        return jsonify({"error": "Product already exists"}), 400
    return jsonify({"message": "Product created successfully"}), 201

@app.route('/products/<id>', methods=['DELETE'])
def delete_product(id):
    # Delete the product by ID
    couchbase_client.delete_document_by_key(id)
    return jsonify({"message": "Product deleted successfully"}), 200


if __name__ == '__main__':
    app.run(debug=True)