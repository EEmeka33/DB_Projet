import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from db_mongo import get_films_collection

def plot_hist_films_par_annee():
    col = get_films_collection()
    docs = list(col.find({}, {"year": 1, "_id": 0}))
    df = pd.DataFrame(docs)
    plt.figure(figsize=(10, 4))
    sns.countplot(x="year", data=df)
    plt.xticks(rotation=90)
    plt.tight_layout()
    return plt.gcf()

def plot_scatter_runtime_revenue():
    col = get_films_collection()
    docs = list(col.find({}, {"Runtime (Minutes)": 1, "Revenue (Millions)": 1, "_id": 0}))
    df = pd.DataFrame(docs)
    df = df[df["Revenue (Millions)"] != ""]
    df["Revenue (Millions)"] = df["Revenue (Millions)"].astype(float)
    plt.figure(figsize=(6, 4))
    sns.scatterplot(x="Runtime (Minutes)", y="Revenue (Millions)", data=df)
    plt.tight_layout()
    return plt.gcf()
