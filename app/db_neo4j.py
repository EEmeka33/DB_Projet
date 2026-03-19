from neo4j import GraphDatabase
from neo4j.exceptions import ServiceUnavailable
from config import NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD
import time

_driver = None

def get_neo4j_driver(retries=12, delay=5):
    global _driver
    if _driver is not None:
        return _driver

    last_exception = None
    for attempt in range(1, retries + 1):
        try:
            _driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))
            _driver.verify_connectivity()
            print(f"Connected to Neo4j ({NEO4J_URI}) on attempt {attempt}.")
            return _driver
        except ServiceUnavailable as exc:
            last_exception = exc
            print(f"Neo4j not available ({NEO4J_URI}) attempt {attempt}/{retries}: {exc}")
            time.sleep(delay)

    raise RuntimeError(
        f"Could not connect to Neo4j at {NEO4J_URI} after {retries} attempts"
    ) from last_exception

def run_query(query, parameters=None):
    driver = get_neo4j_driver()
    with driver.session() as session:
        return list(session.run(query, parameters or {}))
