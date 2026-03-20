# Projet NoSQL ESIEA

## Structure du projet

```
nosql-projet/
├─ docker-compose.yml
├─ .env
├─ data/
│   ├─ movies-2.json
│   ├─ mongo-db/        (sera créé par Mongo)
│   ├─ neo4j-data/      (sera créé par Neo4j)
│   └─ neo4j-logs/      (sera créé par Neo4j)
└─ app/
    ├─ Dockerfile
    ├─ requirements.txt
    ├─ config.py
    ├─ db_mongo.py
    ├─ db_neo4j.py
    ├─ import_mongo.py
    ├─ import_neo4j.py
    ├─ queries_mongo.py
    ├─ queries_neo4j.py
    ├─ analysis.py
    └─ main.py
```

## Description
Un projet d’analyse et d’import de données filmographiques dans MongoDB et Neo4j avec une API Streamlit.

Le projet inclut :
- import des données JSON vers MongoDB (`import_mongo.py`) et Neo4j (`import_neo4j.py`).
- stockage des données dans des bases MongoDB et Neo4j dans `data/mongo-db` et `data/neo4j-data`.
- requêtage pour affichage.

## Structure du dépôt

- `docker-compose.yml` : Mise en place des services `mongo`, `neo4j`, et `app`.
- `app/` : code applicatif Python.
  - `main.py` : point d’entrée de l’application Streamlit.
  - `config.py` : configuration les variables et constantes (host/port, chemins, etc.).
  - `db_mongo.py` : connexion et helpers MongoDB.
  - `db_neo4j.py` : connexion et helpers Neo4j.
  - `import_mongo.py` : import JSON dans MongoDB.
  - `import_neo4j.py` : import JSON dans Neo4j.
  - `queries_mongo.py`, `queries_neo4j.py` : requêtes métier pour chaque base.
  - `analysis.py` : fonctions d'affichage des graphes.
  - `requirements.txt` : dépendances Python.
  - `Dockerfile` : construction de l'image de service applicatif.
- `data/` : fichiers de données et magasins d’état de bases.
  - `movies-2.json` : dataset source.
  - `mongo-db/`, `neo4j-data/` : dossiers de données persistantes.

## Pré-requis

- Docker + Docker Compose.

## Installation et exécution

1. Ouvrir un terminal à la racine du repo.
2. Lancer `docker-compose build app`.
3. Lancer `docker-compose up -d`.
4. Attendre que les conteneurs `mongo` et `neo4j` soient en bonne santé (check logs et `docker ps`).
5. Le service streamlit pour le frontend est exposé sur `http://localhost:8501`.

## Structure globale de l’application

1. Connexions à Mongo/Neo4j gérées dans `db_mongo.py` et `db_neo4j.py`.
2. Import des documents JSON depuis `movies-2.json`.
3. Requêtes et transformations pour comparaison des deux bases.
4. Tableau de bord/visualisation des résultats.

## Commandes utiles

- `docker-compose down` pour arrêter et supprimer les conteneurs.
- `docker-compose up -d --build` pour rebuild à froid.
- `docker-compose logs -f app` pour suivre le log de l’application.

## Notes

- Assurez-vous que les ports `27017`, `7474`, `7687`, `8501` sont libres.
- Les dossiers de données sont montés en volumes pour persistance.
- Le mot de passe Neo4j est `password` comme indiqué dans `docker-compose.yml`.
