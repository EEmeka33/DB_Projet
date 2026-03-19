from db_mongo import get_films_collection
from db_neo4j import get_neo4j_driver

def import_to_neo4j():
    col = get_films_collection()
    driver = get_neo4j_driver()

    films = list(col.find({}))

    try:
        with driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (f:Film) REQUIRE (f.id) IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (a:Actor) REQUIRE (a.name) IS UNIQUE")
            session.run("CREATE CONSTRAINT IF NOT EXISTS FOR (r:Realisateur) REQUIRE (r.name) IS UNIQUE")

            for film in films:
                film_id = str(film.get("_id"))
                title = film.get("title")
                year = film.get("year")
                votes = film.get("Votes")
                revenue = film.get("Revenue (Millions)")
                rating = film.get("rating")
                metascore = film.get("Metascore")
                director = film.get("Director")
                genre_text = film.get("genre")
                actors = [
                    a.strip()
                    for a in film.get("Actors", "").split(",")
                    if a.strip()
                ]

                if isinstance(revenue, str):
                    try:
                        revenue = float(revenue)
                    except ValueError:
                        revenue = None

                session.run(
                    """
                    MERGE (f:Film {id: $id})
                    SET f.title     = $title,
                        f.year      = $year,
                        f.votes     = $votes,
                        f.revenue   = $revenue,
                        f.rating    = $rating,
                        f.metascore = $metascore,
                        f.director  = $director,
                        f.genre     = $genre_text
                    """,
                    {
                        "id": film_id,
                        "title": title,
                        "year": year,
                        "votes": votes,
                        "revenue": revenue,
                        "rating": rating,
                        "metascore": metascore,
                        "director": director,
                        "genre_text": genre_text,
                    },
                )

                if director:
                    session.run(
                        """
                        MERGE (r:Realisateur {name: $name})
                        MERGE (f:Film {id: $film_id})
                        MERGE (r)-[:A_REALISE]->(f)
                        """,
                        {"name": director, "film_id": film_id},
                    )

                for actor in actors:
                    session.run(
                        """
                        MERGE (a:Actor {name: $name})
                        MERGE (f:Film {id: $film_id})
                        MERGE (a)-[:A_JOUE]->(f)
                        """,
                        {"name": actor, "film_id": film_id},
                    )

    except Exception as e:
        print("Erreur lors de l'import Neo4j :", str(e))
        raise

    print("Import Neo4j terminé avec modèle simplifié.")

if __name__ == "__main__":
    import_to_neo4j()
