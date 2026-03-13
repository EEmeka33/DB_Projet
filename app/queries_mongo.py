from db_mongo import get_films_collection
import pandas as pd

col = get_films_collection()

def q1_annee_plus_de_films():
    pipeline = [
        {"$group": {"_id": "$year", "nb": {"$sum": 1}}},
        {"$sort": {"nb": -1}},
        {"$limit": 1},
    ]
    return list(col.aggregate(pipeline))

def q2_nb_films_apres_1999():
    return col.count_documents({"year": {"$gt": 1999}})

def q3_moyenne_votes_2007():
    pipeline = [
        {"$match": {"year": 2007}},
        {"$group": {"_id": None, "moy_votes": {"$avg": "$Votes"}}},
    ]
    res = list(col.aggregate(pipeline))
    return res[0]["moy_votes"] if res else None

def q4_hist_films_par_annee():
    pipeline = [
        {"$group": {"_id": "$year", "nb": {"$sum": 1}}},
        {"$sort": {"_id": 1}},
    ]
    return list(col.aggregate(pipeline))

def q5_genres_disponibles():
    genres = col.distinct("genre")
    s = set()
    for g in genres:
        if not g:
            continue
        for part in g.split(","):
            s.add(part.strip())
    return sorted(s)

def q6_film_max_revenue():
    pipeline = [
        {"$addFields": {
            "rev_num": {
                "$cond": [
                    {"$or": [
                        {"$eq": ["$Revenue (Millions)", ""]},
                        {"$eq": ["$Revenue (Millions)", None]}
                    ]},
                    None,
                    {"$toDouble": "$Revenue (Millions)"}
                ]
            }
        }},
        {"$sort": {"rev_num": -1}},
        {"$limit": 1},
    ]
    return list(col.aggregate(pipeline))

def q7_realisateurs_plus_de_5_films():
    pipeline = [
        {"$group": {"_id": "$Director", "nb": {"$sum": 1}}},
        {"$match": {"nb": {"$gt": 5}}},
        {"$sort": {"nb": -1}},
    ]
    return list(col.aggregate(pipeline))

def q8_genre_plus_rentable_moyenne():
    docs = list(col.find(
        {"Revenue (Millions)": {"$ne": ""}},
        {"genre": 1, "Revenue (Millions)": 1, "_id": 0}
    ))
    rows = []
    for d in docs:
        rev = d.get("Revenue (Millions)")
        if rev in (None, ""):
            continue
        for g in d.get("genre", "").split(","):
            rows.append({"genre": g.strip(), "rev": float(rev)})
    df = pd.DataFrame(rows)
    return df.groupby("genre")["rev"].mean().sort_values(ascending=False)

def q9_top3_films_par_decennie():
    docs = list(col.find({}, {"title": 1, "year": 1, "rating": 1, "_id": 0}))
    df = pd.DataFrame(docs)
    df["decade"] = (df["year"] // 10) * 10
    res = {}
    for dec, sub in df.groupby("decade"):
        res[dec] = sub.sort_values("rating", ascending=False).head(3)
    return res

def q10_film_le_plus_long_par_genre():
    docs = list(col.find({}, {"genre": 1, "Runtime (Minutes)": 1, "title": 1}))
    rows = []
    for d in docs:
        runtime = d.get("Runtime (Minutes)")
        if runtime is None:
            continue
        for g in d.get("genre", "").split(","):
            rows.append({"genre": g.strip(), "runtime": runtime, "title": d["title"]})
    df = pd.DataFrame(rows)
    idx = df.groupby("genre")["runtime"].idxmax()
    return df.loc[idx]

def q11_vue_films_bons_scores():
    filtre = {
        "Metascore": {"$gt": 80},
        "Revenue (Millions)": {"$ne": ""},
    }
    return list(col.find(filtre))

def q12_correlation_runtime_revenue():
    docs = list(col.find({}, {"Runtime (Minutes)": 1, "Revenue (Millions)": 1, "_id": 0}))
    df = pd.DataFrame(docs)
    df = df[df["Revenue (Millions)"] != ""]
    df["Revenue (Millions)"] = df["Revenue (Millions)"].astype(float)
    df = df.dropna()
    return df["Runtime (Minutes)"].corr(df["Revenue (Millions)"])

def q13_evolution_duree_moyenne_par_decennie():
    docs = list(col.find({}, {"year": 1, "Runtime (Minutes)": 1, "_id": 0}))
    df = pd.DataFrame(docs).dropna()
    df["decade"] = (df["year"] // 10) * 10
    return df.groupby("decade")["Runtime (Minutes)"].mean().sort_index()
