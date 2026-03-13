from pymongo import MongoClient
from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION

_client = None

def get_mongo_client():
    global _client
    if _client is None:
        _client = MongoClient(MONGO_URI)
    return _client

def get_films_collection():
    client = get_mongo_client()
    db = client[MONGO_DB]
    return db[MONGO_COLLECTION]
