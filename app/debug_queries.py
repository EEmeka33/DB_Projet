# debug_queries.py
import pprint
from pymongo import MongoClient
from neo4j import GraphDatabase

# -------------------------------------------------------------------
# CONFIG (adapte si besoin)
# -------------------------------------------------------------------
MONGO_URI = "mongodb://mongo:27017"
MONGO_DB = "entertainment"
MONGO_COLLECTION = "films"

NEO4J_URI = "bolt://neo4j:7687"
NEO4J_USER = "neo4j"
NEO4J_PASSWORD = "password"

pp = pprint.PrettyPrinter(indent=2)


# -------------------------------------------------------------------
# MONGO – CONNEXION
# -------------------------------------------------------------------
def get_mongo_col():
    client = MongoClient(MONGO_URI)
    db = client[MONGO_DB]
    return db[MONGO_COLLECTION]


# -------------------------------------------------------------------
# NEO4J – CONNEXION
# -------------------------------------------------------------------
driver = GraphDatabase.driver(NEO4J_URI, auth=(NEO4J_USER, NEO4J_PASSWORD))


def run_cypher(query, params=None):
    with driver.session() as session:
        return list(session.run(query, params or {}))


# -------------------------------------------------------------------
# MONGO QUERIES 1–13 (en Cypher shell on affiche juste les résultats)
# -------------------------------------------------------------------
def debug_mongo():
    col = get_mongo_col()

    print("\n================= MONGO – Q1 =================")
    try:
        res = col.aggregate([
            {"$group": {"_id": "$year", "nbFilms": {"$sum": 1}}},
            {"$sort": {"nbFilms": -1}},
            {"$limit": 1},
        ])
        pp.pprint(list(res))
    except Exception as e:
        print("ERREUR Q1:", e)

    print("\n================= MONGO – Q2 =================")
    try:
        print(col.count_documents({"year": {"$gt": 1999}}))
    except Exception as e:
        print("ERREUR Q2:", e)

    print("\n================= MONGO – Q3 =================")
    try:
        res = col.aggregate([
            {"$match": {"year": 2007}},
            {"$group": {"_id": None, "moy_votes": {"$avg": "$Votes"}}},
        ])
        pp.pprint(list(res))
    except Exception as e:
        print("ERREUR Q3:", e)

    print("\n================= MONGO – Q4 =================")
    try:
        res = col.aggregate([
            {"$group": {"_id": "$year", "nbFilms": {"$sum": 1}}},
            {"$sort": {"_id": 1}},
        ])
        pp.pprint(list(res))
    except Exception as e:
        print("ERREUR Q4:", e)

    print("\n================= MONGO – Q5 =================")
    try:
        pp.pprint(col.distinct("genre"))
    except Exception as e:
        print("ERREUR Q5:", e)

    print("\n================= MONGO – Q6 =================")
    try:
        res = col.aggregate([
            {
                "$addFields": {
                    "revNum": {
                        "$cond": [
                            {
                                "$or": [
                                    {"$eq": ["$Revenue (Millions)", ""]},
                                    {"$eq": ["$Revenue (Millions)", None]},
                                ]
                            },
                            None,
                            {"$toDouble": "$Revenue (Millions)"},
                        ]
                    }
                }
            },
            {"$sort": {"revNum": -1}},
            {"$limit": 1},
        ])
        pp.pprint(list(res))
    except Exception as e:
        print("ERREUR Q6:", e)

    print("\n================= MONGO – Q7 =================")
    try:
        res = col.aggregate([
            {"$group": {"_id": "$Director", "nbFilms": {"$sum": 1}}},
            {"$match": {"nbFilms": {"$gt": 5}}},
            {"$sort": {"nbFilms": -1}},
        ])
        pp.pprint(list(res))
    except Exception as e:
        print("ERREUR Q7:", e)

    print("\n================= MONGO – Q8 =================")
    try:
        docs = list(col.find(
            {"Revenue (Millions)": {"$nin": ["", None]}},
            {"genre": 1, "Revenue (Millions)": 1, "_id": 0},
        ))
        pp.pprint(docs[:5])
        print("... (analyse détaillée à faire en Python/pandas)")
    except Exception as e:
        print("ERREUR Q8:", e)

    print("\n================= MONGO – Q9 =================")
    try:
        docs = list(col.find({}, {"title": 1, "year": 1, "rating": 1, "_id": 0}))
        pp.pprint(docs[:5])
        print("... (tri/top3 par décennie à faire en Python pandas)")
    except Exception as e:
        print("ERREUR Q9:", e)

    print("\n================= MONGO – Q10 =================")
    try:
        docs = list(col.find({}, {"genre": 1, "Runtime (Minutes)": 1, "title": 1}))
        pp.pprint(docs[:5])
        print("... (max par genre à faire en Python pandas)")
    except Exception as e:
        print("ERREUR Q10:", e)

    print("\n================= MONGO – Q11 =================")
    try:
        res = col.find(
            {
                "Metascore": {"$gt": 80},
                "Revenue (Millions)": {"$nin": ["", None]},
            },
            {"title": 1, "Metascore": 1, "Revenue (Millions)": 1},
        )
        pp.pprint(list(res))
    except Exception as e:
        print("ERREUR Q11:", e)

    print("\n================= MONGO – Q12 =================")
    try:
        docs = list(col.find({}, {"Runtime (Minutes)": 1, "Revenue (Millions)": 1}))
        pp.pprint(docs[:5])
        print("... (corrélation à faire en Python pandas)")
    except Exception as e:
        print("ERREUR Q12:", e)

    print("\n================= MONGO – Q13 =================")
    try:
        docs = list(col.find({}, {"year": 1, "Runtime (Minutes)": 1}))
        pp.pprint(docs[:5])
        print("... (moyenne par décennie à faire en Python pandas)")
    except Exception as e:
        print("ERREUR Q13:", e)


# -------------------------------------------------------------------
# NEO4J QUERIES 14–30
# -------------------------------------------------------------------
def debug_neo4j():
    print("\n================= NEO4J – Q14 =================")
    try:
        res = run_cypher("""
            MATCH (a:Actor)-[:A_JOUE]->(f:Film)
            RETURN a.name AS acteur, count(f) AS nbFilms
            ORDER BY nbFilms DESC
            LIMIT 1
        """)
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q14:", e)

    print("\n================= NEO4J – Q15 =================")
    try:
        res = run_cypher("""
            MATCH (anne:Actor {name: "Anne Hathaway"})-[:A_JOUE]->(f:Film)<-[:A_JOUE]-(a:Actor)
            WHERE a.name <> "Anne Hathaway"
            RETURN DISTINCT a.name AS acteur
            ORDER BY acteur
        """)
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q15:", e)

    print("\n================= NEO4J – Q16 =================")
    try:
        res = run_cypher("""
            MATCH (a:Actor)-[:A_JOUE]->(f:Film)
            WHERE f.revenue IS NOT NULL
            RETURN a.name AS acteur, sum(f.revenue) AS totalRevenus
            ORDER BY totalRevenus DESC
            LIMIT 1
        """)
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q16:", e)

    print("\n================= NEO4J – Q17 =================")
    try:
        res = run_cypher("""
            MATCH (f:Film)
            RETURN avg(f.votes) AS moyenneVotes
        """)
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q17:", e)

    print("\n================= NEO4J – Q18 =================")
    try:
        res = run_cypher("""
            MATCH (f:Film)
            WITH f, split(f.genre, ",") AS genres
            UNWIND genres AS g
            WITH trim(g) AS genre
            RETURN genre, count(*) AS nbFilms
            ORDER BY nbFilms DESC
            LIMIT 1
        """)
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q18:", e)

    print("\n================= NEO4J – Q19 =================")
    try:
        res = run_cypher("""
            MATCH (me:Actor {name: $nom})-[:A_JOUE]->(f1:Film)<-[:A_JOUE]-(a:Actor)
            MATCH (a)-[:A_JOUE]->(f2:Film)
            WHERE NOT (me)-[:A_JOUE]->(f2)
            RETURN DISTINCT f2.title AS film, f2.year AS annee
            ORDER BY annee, film
        """, {"nom": "Hugo"})
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q19:", e)

    print("\n================= NEO4J – Q20 =================")
    try:
        res = run_cypher("""
            MATCH (r:Realisateur)-[:A_REALISE]->(f:Film)<-[:A_JOUE]-(a:Actor)
            RETURN r.name AS realisateur, count(DISTINCT a) AS nbActeurs
            ORDER BY nbActeurs DESC
            LIMIT 1
        """)
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q20:", e)

    print("\n================= NEO4J – Q21 =================")
    try:
        res = run_cypher("""
            MATCH (f:Film)<-[:A_JOUE]-(a:Actor)-[:A_JOUE]->(autre:Film)
            WHERE f <> autre
            WITH f, count(DISTINCT autre) AS nbConnexions
            RETURN f.title AS film, nbConnexions
            ORDER BY nbConnexions DESC
            LIMIT 10
        """)
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q21:", e)

    print("\n================= NEO4J – Q22 =================")
    try:
        res = run_cypher("""
            MATCH (a:Actor)-[:A_JOUE]->(f:Film)<-[:A_REALISE]-(r:Realisateur)
            RETURN a.name AS acteur, count(DISTINCT r) AS nbRealisateurs
            ORDER BY nbRealisateurs DESC
            LIMIT 5
        """)
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q22:", e)

    print("\n================= NEO4J – Q23 =================")
    try:
        res = run_cypher("""
            MATCH (a:Actor {name: $nom})-[:A_JOUE]->(f:Film)
            WITH a, collect(DISTINCT f.genre) AS genresPrefs
            MATCH (rec:Film)
            WHERE rec.genre IN genresPrefs
              AND NOT (a)-[:A_JOUE]->(rec)
            RETURN DISTINCT rec.title AS film, rec.year AS annee, rec.genre AS genre
            ORDER BY annee DESC
            LIMIT 10
        """, {"nom": "Anne Hathaway"})
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q23:", e)

    print("\n================= NEO4J – Q24 (sans IN_GENRE/Genre) =================")
    try:
        res = run_cypher("""
            MATCH (r1:Realisateur)-[:A_REALISE]->(f1:Film),
                  (r2:Realisateur)-[:A_REALISE]->(f2:Film)
            WHERE r1 <> r2
              AND f1.genre IS NOT NULL
              AND f1.genre = f2.genre
        RETURN DISTINCT r1.name AS source, r2.name AS cible, f1.genre AS genre
        LIMIT 20
        """)
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q24:", e)

    print("\n================= NEO4J – Q25 =================")
    try:
        res = run_cypher("""
            MATCH (a1:Actor {name: $a1}), (a2:Actor {name: $a2}),
                  p = shortestPath( (a1)-[:A_JOUE*..6]-(a2) )
            RETURN p
        """, {"a1": "Tom Hanks", "a2": "Scarlett Johansson"})
        pp.pprint(res)
    except Exception as e:
        print("ERREUR Q25:", e)

    print("\n================= NEO4J – Q26 (simple) =================")
    try:
        res = run_cypher("""
            MATCH (a:Actor)-[:A_JOUE]->(f:Film)<-[:A_JOUE]-(autre:Actor)
            WHERE a <> autre
            WITH a, collect(DISTINCT autre.name) AS voisins
            RETURN a.name AS acteur, voisins
            ORDER BY size(voisins) DESC, acteur
            LIMIT 20
        """)
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q26:", e)

    print("\n================= NEO4J – Q27 (genre string) =================")
    try:
        res = run_cypher("""
            MATCH (f1:Film), (f2:Film)
            WHERE f1 <> f2
              AND f1.director <> f2.director
              AND f1.genre IS NOT NULL
              AND f1.genre = f2.genre
            RETURN DISTINCT
              f1.title AS film1, f1.director AS real1,
              f2.title AS film2, f2.director AS real2,
              f1.genre AS genre
            LIMIT 20
        """)
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q27:", e)

    print("\n================= NEO4J – Q28 =================")
    try:
        res = run_cypher("""
            MATCH (a:Actor {name: $nom})-[:A_JOUE]->(f:Film)
            WHERE f.genre IS NOT NULL
            WITH a, collect(DISTINCT f.genre) AS genresPrefs
            MATCH (rec:Film)
            WHERE rec.genre IN genresPrefs
              AND NOT (a)-[:A_JOUE]->(rec)
            RETURN DISTINCT rec.title AS film, rec.year AS annee, rec.genre AS genre
            ORDER BY annee DESC
            LIMIT 20
        """, {"nom": "Anne Hathaway"})
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q28:", e)

    print("\n================= NEO4J – Q29 =================")
    try:
        res = run_cypher("""
            MATCH (r1:Realisateur)-[:A_REALISE]->(f1:Film),
                  (r2:Realisateur)-[:A_REALISE]->(f2:Film)
            WHERE r1 <> r2
              AND f1.year = f2.year
              AND f1.genre IS NOT NULL
              AND f1.genre = f2.genre
            RETURN DISTINCT r1.name AS source, r2.name AS cible,
                            f1.year AS annee, f1.genre AS genre
            LIMIT 20
        """)
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q29:", e)

    print("\n================= NEO4J – Q30 =================")
    try:
        res = run_cypher("""
            MATCH (r:Realisateur)-[:A_REALISE]->(f:Film)<-[:A_JOUE]-(a:Actor)
            WHERE f.revenue IS NOT NULL
            WITH r, a,
                 count(f) AS nbFilms,
                 avg(f.revenue) AS revenuMoyen,
                 avg(f.rating)  AS noteMoyenne
            WHERE nbFilms >= 1
            RETURN
              r.name AS realisateur,
              a.name AS acteur,
              nbFilms,
              revenuMoyen,
              noteMoyenne
            ORDER BY nbFilms DESC, revenuMoyen DESC
            LIMIT 20
        """)
        pp.pprint([dict(r) for r in res])
    except Exception as e:
        print("ERREUR Q30:", e)


if __name__ == "__main__":
    print("===== DEBUG MONGODB =====")
    debug_mongo()
    print("\n\n===== DEBUG NEO4J =====")
    debug_neo4j()
