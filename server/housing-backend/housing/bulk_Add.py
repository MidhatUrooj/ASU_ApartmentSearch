from elasticsearch import Elasticsearch, helpers
import json

# Elastic Cloud connection
es = Elasticsearch(
    "https://555a729a7910477e9d58b4aecd79dc92.us-central1.gcp.cloud.es.io:443",
    api_key="enNqVWRwTUIxU1BUVGl3NVVJNjQ6T3lKYmRPY01UeldtTThxNGo3dmxCUQ=="
)

# Define the index name
INDEX_NAME = "housing"

# Function to create the index with mapping
def create_index():
    mapping = {
        "mappings": {
            "properties": {
                "title": {"type": "text"},
                "description": {"type": "text"},
                "location": {"type": "keyword"},
                "price": {"type": "float"},
                "bhk": {"type": "keyword"},
                "amenities": {"type": "keyword"},
                "area_sqft": {"type": "float"},
                "construction_year": {"type": "integer"},
                "property_type": {"type": "keyword"}
            }
        }
    }

    # Delete the index if it already exists
    if es.indices.exists(index=INDEX_NAME):
        es.indices.delete(index=INDEX_NAME)
        print(f"Deleted existing index '{INDEX_NAME}'.")

    # Create the new index
    es.indices.create(index=INDEX_NAME, body=mapping)
    print(f"Created index '{INDEX_NAME}' with mapping.")

# Function to validate and clean documents
def validate_documents(documents):
    cleaned_documents = []
    for doc in documents:
        try:
            cleaned_doc = {
                "title": doc.get("title", "").strip(),
                "description": doc.get("description", "").strip(),
                "location": doc.get("location", "").strip(),
                "price": float(doc.get("price", 0.0)),
                "bhk": doc.get("bhk", "").strip(),
                "amenities": doc.get("amenities", []),
                "area_sqft": float(doc.get("area_sqft", 0.0)),
                "construction_year": int(doc.get("construction_year", 0)),
                "property_type": doc.get("property_type", "").strip()
            }
            cleaned_documents.append(cleaned_doc)
        except Exception as e:
            print(f"Error processing document: {doc}")
            print(f"Error: {e}")
    return cleaned_documents

# Function to bulk insert documents into Elasticsearch
def bulk_insert_documents(documents):
    actions = [
        {
            "_index": INDEX_NAME,
            "_source": doc,
        }
        for doc in documents
    ]

    try:
        helpers.bulk(es, actions)
        print(f"Successfully inserted {len(documents)} documents into '{INDEX_NAME}'.")
    except Exception as e:
        print(f"Error during bulk insert: {e}")

# Main script
if __name__ == "__main__":
    # Step 1: Create the index
    create_index()

    # Step 2: Load housing data from JSON file
    with open("housing.json", "r") as f:
        raw_documents = json.load(f)

    # Step 3: Validate and clean the data
    documents = validate_documents(raw_documents)

    # Step 4: Insert documents into Elasticsearch
    bulk_insert_documents(documents)