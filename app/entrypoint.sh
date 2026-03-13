#!/usr/bin/env bash
set -e  # stop si une commande échoue

echo "=== Import MongoDB ==="
python import_mongo.py

echo "=== Import Neo4j ==="
python import_neo4j.py

echo "=== Lancement de Streamlit ==="
exec streamlit run main.py --server.address 0.0.0.0 --server.port 8501
