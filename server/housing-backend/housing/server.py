from flask import Flask, request, jsonify
from flask_cors import CORS
from elasticsearch import Elasticsearch

app = Flask(__name__)
CORS(app)

# Connect to Elastic Cloud
es = Elasticsearch(
    "endpoint",
    api_key="key"
)

# Elasticsearch index name
INDEX_NAME = "housing"
@app.route("/search", methods=["GET"])
def search():
    query = request.args.get("query", "").strip()
    page = int(request.args.get("page", 1))
    page_size = int(request.args.get("page_size", 10))

    # Capture all query parameters for debugging
    filters = request.args.to_dict()  # Convert query parameters to a dictionary
    print(f"Query Parameters: {filters}")  # Debugging

    # Extract filters if present
    filters = {key: value for key, value in filters.items() if key not in ["query", "page", "page_size"]}
    print(f"Extracted Filters: {filters}")  # Debugging

    # Build Elasticsearch query
    es_query = {
        "size": page_size,
        "from": (page - 1) * page_size,
        "query": {
            "bool": {
                "must": [],
                "filter": []
            }
        }
    }

    # Add `match_all` if query is empty, otherwise use `multi_match`
    if query:
        es_query["query"]["bool"]["must"].append({
            "multi_match": {
                "query": query,
                "fields": ["title^3", "description^2", "location", "amenities"],
                "fuzziness": "AUTO"
            }
        })
    else:
        es_query["query"]["bool"]["must"].append({"match_all": {}})

    # Add filters to the query
    if filters.get("location"):
        es_query["query"]["bool"]["filter"].append({"term": {"location": filters["location"]}})
    if filters.get("bhk"):
        es_query["query"]["bool"]["filter"].append({"term": {"bhk": filters["bhk"]}})
    if filters.get("min_price") or filters.get("max_price"):
        price_filter = {"range": {"price": {}}}
        if filters.get("min_price"):
            price_filter["range"]["price"]["gte"] = float(filters["min_price"])
        if filters.get("max_price"):
            price_filter["range"]["price"]["lte"] = float(filters["max_price"])
        es_query["query"]["bool"]["filter"].append(price_filter)
    if filters.get("amenities"):
        es_query["query"]["bool"]["filter"].append({"terms": {"amenities": filters["amenities"].split(",")}})
    if filters.get("property_type"):
        es_query["query"]["bool"]["filter"].append({"term": {"property_type": filters["property_type"]}})
    if filters.get("construction_year"):
        es_query["query"]["bool"]["filter"].append({
            "range": {
                "construction_year": {"gte": int(filters["construction_year"])}
            }
        })

    # Print the generated Elasticsearch query for debugging
    print("Generated Elasticsearch Query:")
    print(es_query)

    # Execute search query
    try:
        response = es.search(index=INDEX_NAME, body=es_query)
        # Print the raw response for debugging
        print("Elasticsearch Response:")
        print(response.body)
        return jsonify(response.body)  # Extract the raw data using `.body`
    except Exception as e:
        print(f"Error executing query: {e}")
        return jsonify({"error": str(e)}), 500



if __name__ == "__main__":
    app.run(debug=True)
