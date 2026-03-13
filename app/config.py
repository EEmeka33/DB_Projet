import os
from dotenv import load_dotenv

# Charge les variables du .env (monté dans le conteneur)
load_dotenv()

MONGO_URI = os.getenv("MONGO_URI", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "entertainment")
MONGO_COLLECTION = os.getenv("MONGO_COLLECTION", "films")

NEO4J_URI = os.getenv("NEO4J_URI", "bolt://neo4j:7687")
NEO4J_USER = os.getenv("NEO4J_USER", "neo4j")
NEO4J_PASSWORD = os.getenv("NEO4J_PASSWORD", "password")
