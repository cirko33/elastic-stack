from elasticsearch import Elasticsearch, helpers
from os import environ

elastic_password = environ.get("ELASTIC_PASSWORD")
basic_auth = None if elastic_password is None else ('elastic', elastic_password)

client = Elasticsearch(
    ["http://elasticsearch:9200/"],
    basic_auth=basic_auth
)

index = "logs-generic-default"
# region narcotics

# target_index = "narcotics-count-per-year"

# query = {
#     "size": 0,
#     "aggs": {
#         "years": {
#             "terms": {
#                 "field": "Year",
#             },
#             "aggs": {
#                 "narcotics_count": {
#                     "filter": {
#                         "term": {
#                             "Primary Type": "NARCOTICS"
#                         }
#                     }
#                 }
#             }
#         }
#     }
# }

# response = client.search(index=index, body=query)

# buckets = response["aggregations"]["years"]["buckets"]

# documents = []
# for bucket in buckets:
#     year = bucket["key"]
#     narcotics_count = bucket["narcotics_count"]["doc_count"]
#     doc = {
#         "Year": year,
#         "NarcoticsCount": narcotics_count
#     }
#     documents.append(doc)

# for doc in documents:
#     client.index(index=target_index, body=doc)

# print(f"Documents indexed to '{target_index}' index.")
# endregion narcotics 

# region theft

# Define the query
# query = {
#     "query": {
#         "bool": {
#             "must": [
#                 { "match": { "Location Description": "STREET" } },
#                 { "match": { "Primary Type": "THEFT" } }
#             ]
#         }
#     }
# }

# response = client.count(index=index, body=query)

# # Get the count
# theft_count = response["count"]

# print(f"Number of thefts that happened on the street: {theft_count}")

# endregion theft

# region range-query
index = "logs-generic-default"

target_index = "crack_possession_north"
client.indices.create(index=target_index)
print(f"Created index '{target_index}'.")

mapping = {
    "properties": {
        "location_combined": {
            "type": "geo_point"  
        }
    }
}
client.indices.put_mapping(index=target_index, body=mapping)
print(f"Updated mapping for the 'location_combined' field in index '{target_index}'.")

query = {
    "query": {
        "bool": {
            "must": [
                {"match": {"Description": "POSS: CRACK"}},
                {"range": {"Community Area": {"gte": 1.0, "lte": 22.0}}},
                {"match": {"Primary Type": "NARCOTICS"}}
            ]
        }
    },
    "size": 10000,
    "_source": ["location"]  
}

response = client.search(index=index, body=query)
hits = response["hits"]["hits"]

actions = [
    {
        "_index": target_index,  
        "_source": doc['_source']
    }
    for doc in hits
]
helpers.bulk(client, actions)
print(f"Indexed {len(actions)} documents to '{target_index}'.")

query = {
    "query": {
        "exists": {"field": "location.lat"}  
    }
}

response = client.search(index=target_index, body=query, size=10000)
hits = response["hits"]["hits"]

for hit in hits:
    doc_id = hit["_id"]
    source = hit["_source"]
    
    if "location" in source and "lat" in source["location"] and "lon" in source["location"]:
        lat = source["location"]["lat"]
        lon = source["location"]["lon"]
        source["location_combined"] = {"lat": lat, "lon": lon}
        
        client.index(index=target_index, id=doc_id, body=source)

print(f"Updated documents in index '{target_index}' to include the combined 'location_combined' field.")
# endregion range-query