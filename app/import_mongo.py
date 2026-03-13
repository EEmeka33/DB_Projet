import json
import os
from pymongo import MongoClient
from config import MONGO_URI, MONGO_DB, MONGO_COLLECTION

DATA_PATH = "/data/movies-2.json"

def import_movies():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    col = db[MONGO_COLLECTION]

    col.drop()

    with open(DATA_PATH, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            doc = json.loads(line)
            col.insert_one(doc)

    print("Import Mongo terminé.")

if __name__ == "__main__":
    import_movies()
