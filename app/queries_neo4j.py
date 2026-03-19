from db_neo4j import run_query


def q14_acteur_plus_de_films():
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)
    RETURN a.name AS acteur, count(f) AS nbFilms
    ORDER BY nbFilms DESC
    LIMIT 1
    """
    return run_query(query)


def q15_acteurs_ayant_joue_avec_anne_hathaway():
    query = """
    MATCH (anne:Actor {name: "Anne Hathaway"})-[:A_JOUE]->(f:Film)<-[:A_JOUE]-(a:Actor)
    WHERE a.name <> "Anne Hathaway"
    RETURN DISTINCT a.name AS acteur
    ORDER BY acteur
    """
    return run_query(query)


def q16_acteur_revenus_max():
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)
    WHERE f.revenue IS NOT NULL
    RETURN a.name AS acteur, sum(f.revenue) AS totalRevenus
    ORDER BY totalRevenus DESC
    LIMIT 1
    """
    return run_query(query)


def q17_moyenne_votes():
    query = """
    MATCH (f:Film)
    RETURN avg(f.votes) AS moyenneVotes
    """
    return run_query(query)


def q18_genre_le_plus_represente():
    query = """
    MATCH (f:Film)
    WITH f, split(f.genre, ",") AS genres
    UNWIND genres AS g
    WITH trim(g) AS genre
    RETURN genre, count(*) AS nbFilms
    ORDER BY nbFilms DESC
    LIMIT 1
    """
    return run_query(query)


# ------------------------- Q19 -------------------------


def q19_films_avec_acteurs_ayant_joue_avec_vous(nom_acteur):
    """
    Q19 : Quels sont les films dans lesquels les acteurs ayant joué avec vous
    ont également joué ?
    - nom_acteur : nom de l'acteur qui vous représente dans le graphe (Scarlett Johansson, par exemple).
    """
    query = """
    MATCH (me:Actor {name: $nom})-[:A_JOUE]->(f1:Film)<-[:A_JOUE]-(a:Actor)
    MATCH (a)-[:A_JOUE]->(f2:Film)
    WHERE NOT (me)-[:A_JOUE]->(f2)
    RETURN DISTINCT f2.title AS film, f2.year AS annee
    ORDER BY annee, film
    """
    return run_query(query, {"nom": nom_acteur})


# ------------------------- Q20 -------------------------


def q20_realisateur_plus_d_acteurs_distincts():
    query = """
    MATCH (r:Realisateur)-[:A_REALISE]->(f:Film)<-[:A_JOUE]-(a:Actor)
    RETURN r.name AS realisateur, count(DISTINCT a) AS nbActeurs
    ORDER BY nbActeurs DESC
    LIMIT 1
    """
    return run_query(query)


# ------------------------- Q21 -------------------------


def q21_films_les_plus_connectes():
    query = """
    MATCH (f:Film)<-[:A_JOUE]-(a:Actor)-[:A_JOUE]->(autre:Film)
    WHERE f <> autre
    WITH f, count(DISTINCT autre) AS nbConnexions
    RETURN f.title AS film, nbConnexions
    ORDER BY nbConnexions DESC
    LIMIT 10
    """
    return run_query(query)



# ------------------------- Q22 -------------------------


def q22_top5_acteurs_plus_de_realisateurs():
    """
    Q22 : Top 5 acteurs ayant joué avec le plus de réalisateurs différents.
    """
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)<-[:A_REALISE]-(r:Realisateur)
    RETURN a.name AS acteur, count(DISTINCT r) AS nbRealisateurs
    ORDER BY nbRealisateurs DESC
    LIMIT 5
    """
    return run_query(query)


# ------------------------- Q23 -------------------------


def q23_recommande_films_pour_acteur(nom_acteur):
    query = """
    MATCH (a:Actor {name: $nom})-[:A_JOUE]->(f:Film)
    WITH a, collect(DISTINCT f.genre) AS genresPrefs
    MATCH (rec:Film)
    WHERE rec.genre IN genresPrefs
      AND NOT (a)-[:A_JOUE]->(rec)
    RETURN DISTINCT rec.title AS film, rec.year AS annee, rec.genre AS genre
    ORDER BY annee DESC
    LIMIT 10
    """
    return run_query(query, {"nom": nom_acteur})



# ------------------------- Q24 -------------------------


def q24_creer_relations_influence_par():
    """
    Q24 – Relation INFLUENCE_PAR entre réalisateurs en fonction
    de genres de films (champ Film.genre en string).
    """
    query = """
    MATCH (r1:Realisateur)-[:A_REALISE]->(f1:Film)
    MATCH (r2:Realisateur)-[:A_REALISE]->(f2:Film)
    WHERE r1 <> r2
      AND f1.genre IS NOT NULL
      AND f1.genre = f2.genre
    MERGE (r1)-[rel:INFLUENCE_PAR]->(r2)
    ON CREATE SET rel.commonGenres = 1
    ON MATCH SET  rel.commonGenres = rel.commonGenres + 1
    RETURN r1.name AS source, r2.name AS cible, rel.commonGenres AS nbGenres
    ORDER BY nbGenres DESC
    LIMIT 50
    """
    return run_query(query)



# ------------------------- Q25 -------------------------


def chemin_plus_court_entre_acteurs(acteur1, acteur2):
    """
    Q25 : Chemin le plus court entre deux acteurs.
    """
    query = """
    MATCH (a1:Actor {name: $a1}), (a2:Actor {name: $a2}),
          p = shortestPath( (a1)-[:A_JOUE*..6]-(a2) )
    RETURN p
    """
    return run_query(query, {"a1": acteur1, "a2": acteur2})


# ------------------------- Q26 -------------------------


def q26_calculer_communautes_acteurs():
    """
    Q26 – Version simple: grouper les acteurs par 'composantes'
    en fonction des films partagés, sans utiliser GDS.
    On renvoie juste les groupes trouvés (liste d'acteurs reliés).
    """
    query = """
    MATCH (a:Actor)-[:A_JOUE]->(f:Film)<-[:A_JOUE]-(autre:Actor)
    WHERE a <> autre
    WITH a, collect(DISTINCT autre.name) AS voisins
    RETURN a.name AS acteur, voisins
    ORDER BY size(voisins) DESC, acteur
    LIMIT 50
    """
    return run_query(query)



# ------------------------- Q27 -------------------------


def q27_films_genres_communs_realisateurs_diff():
    """
    Q27 – Films qui ont même valeur de Film.genre mais des réalisateurs différents.
    """
    query = """
    MATCH (f1:Film), (f2:Film)
    WHERE f1 <> f2
      AND f1.director <> f2.director
      AND f1.genre IS NOT NULL
      AND f1.genre = f2.genre
    RETURN DISTINCT
      f1.title AS film1, f1.director AS real1,
      f2.title AS film2, f2.director AS real2,
      f1.genre AS genre
    LIMIT 50
    """
    return run_query(query)



# ------------------------- Q28 -------------------------


def q28_recommande_films_selon_preferences_acteur(nom_acteur):
    """
    Q28 – Recommander des films en se basant sur les genres (string)
    des films où l'acteur a déjà joué.
    """
    query = """
    MATCH (a:Actor {name: $nom})-[:A_JOUE]->(f:Film)
    WHERE f.genre IS NOT NULL
    WITH a, collect(DISTINCT f.genre) AS genresPrefs
    MATCH (rec:Film)
    WHERE rec.genre IN genresPrefs
      AND NOT (a)-[:A_JOUE]->(rec)
    RETURN DISTINCT rec.title AS film,
           rec.year AS annee,
           rec.genre AS genre
    ORDER BY annee DESC
    LIMIT 20
    """
    return run_query(query, {"nom": nom_acteur})



# ------------------------- Q29 -------------------------


def q29_creer_relations_concurrence():
    """
    Q29 – Créer une relation de concurrence entre réalisateurs ayant
    réalisé des films du même genre (string) la même année.
    """
    query = """
    MATCH (r1:Realisateur)-[:A_REALISE]->(f1:Film),
          (r2:Realisateur)-[:A_REALISE]->(f2:Film)
    WHERE r1 <> r2
      AND f1.year = f2.year
      AND f1.genre IS NOT NULL
      AND f1.genre = f2.genre
    MERGE (r1)-[c:CONCURRENCE]->(r2)
    ON CREATE SET c.year = f1.year, c.commonGenres = 1
    ON MATCH SET  c.commonGenres = c.commonGenres + 1
    RETURN r1.name AS source, r2.name AS cible,
           c.year AS annee, c.commonGenres AS nbGenres
    ORDER BY nbGenres DESC
    LIMIT 50
    """
    return run_query(query)



# ------------------------- Q30 -------------------------


def q30_collaborations_frequentes_succes():
    """
    Q30 – Collaborations les plus fréquentes entre réalisateurs et acteurs,
    avec revenu moyen et note moyenne (metascore).
    """
    query = """
    MATCH (r:Realisateur)-[:A_REALISE]->(f:Film)<-[:A_JOUE]-(a:Actor)
    WHERE f.revenue IS NOT NULL AND f.metascore IS NOT NULL
    WITH r, a,
         count(f) AS nbFilms,
         avg(toFloat(f.revenue)) AS revenuMoyen,
         avg(toFloat(f.metascore)) AS noteMoyenne
    WHERE nbFilms > 1
    RETURN
      r.name AS realisateur,
      a.name AS acteur,
      nbFilms,
      revenuMoyen,
      noteMoyenne
    ORDER BY nbFilms DESC, revenuMoyen DESC
    LIMIT 50
    """
    return run_query(query)

