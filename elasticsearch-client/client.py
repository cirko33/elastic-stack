from elasticsearch import Elasticsearch
from os import environ

client = Elasticsearch(
    ["http://elasticsearch:9200/"],
    basic_auth=('elastic', environ.get("ELASTIC_PASSWORD"))
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

index = "logs-generic-default"

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
