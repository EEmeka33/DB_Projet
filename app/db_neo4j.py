from neo4j import GraphDatabase
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

_driver = None

def get_neo4j_driver():
    global _driver
    if _driver is None:
        _driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
    return _driver

def run_query(query, parameters=None):
    driver = get_neo4j_driver()
    with driver.session() as session:
        return list(session.run(query, parameters or {}))
