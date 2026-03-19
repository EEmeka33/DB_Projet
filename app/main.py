import streamlit as st

from queries_mongo import (
    q1_annee_plus_de_films,
    q2_nb_films_apres_1999,
    q3_moyenne_votes_2007,
    q4_hist_films_par_annee,             # si tu la gardes côté Python
    q5_genres_disponibles,
    q6_film_max_revenue,
    q7_realisateurs_plus_de_5_films,
    q8_genre_plus_rentable_moyenne,
    q9_top3_films_par_decennie,
    q10_film_le_plus_long_par_genre,
    q11_vue_films_bons_scores,
    q12_correlation_runtime_revenue,
    q13_evolution_duree_moyenne_par_decennie,
)

from analysis import (
    plot_hist_films_par_annee,
    plot_scatter_runtime_revenue,
)

from queries_neo4j import (
    q14_acteur_plus_de_films,
    q15_acteurs_ayant_joue_avec_anne_hathaway,
    q16_acteur_revenus_max,
    q17_moyenne_votes,
    q18_genre_le_plus_represente,
    q19_films_avec_acteurs_ayant_joue_avec_vous,
    q20_realisateur_plus_d_acteurs_distincts,
    q21_films_les_plus_connectes,
    q22_top5_acteurs_plus_de_realisateurs,
    q23_recommande_films_pour_acteur,
    q24_creer_relations_influence_par,
    chemin_plus_court_entre_acteurs,     # Q25
    q26_calculer_communautes_acteurs,
    q27_films_genres_communs_realisateurs_diff,
    q28_recommande_films_selon_preferences_acteur,
    q29_creer_relations_concurrence,
    q30_collaborations_frequentes_succes,
)


st.title("Projet NoSQL ESIEA – MongoDB & Neo4j (Docker)")


section = st.sidebar.selectbox(
    "Section",
    [
        "Mongo – Questions 1–13",
        "Mongo – Visualisations",
        "Neo4j – Questions 14–22",
        "Neo4j – Avancé 23–30",
    ],
)

# --------------------------------------------------------------------
# MONGO – QUESTIONS 1–13
# --------------------------------------------------------------------
if section == "Mongo – Questions 1–13":
    st.header("MongoDB – Questions 1 à 13")

    st.subheader("Q1 – Année avec le plus de films")
    st.write(q1_annee_plus_de_films())

    st.subheader("Q2 – Nombre de films après 1999")
    st.write(q2_nb_films_apres_1999())

    st.subheader("Q3 – Moyenne des votes pour les films sortis en 2007")
    st.write(q3_moyenne_votes_2007())

    st.subheader("Q4 – Histogramme du nombre de films par année")
    st.markdown("Voir l’onglet **Mongo – Visualisations** pour la figure.")

    st.subheader("Q5 – Genres de films disponibles dans la base")
    st.write(q5_genres_disponibles())

    st.subheader("Q6 – Film qui a généré le plus de revenu")
    st.write(q6_film_max_revenue())

    st.subheader("Q7 – Réalisateurs ayant réalisé plus de 5 films")
    st.write(q7_realisateurs_plus_de_5_films())

    st.subheader("Q8 – Genre qui rapporte en moyenne le plus de revenus")
    st.write(q8_genre_plus_rentable_moyenne())

    st.subheader("Q9 – Top 3 films les mieux notés par décennie")
    top3 = q9_top3_films_par_decennie()
    for decennie, df in top3.items():
        st.markdown(f"**Décennie {decennie}**")
        st.dataframe(df)

    st.subheader("Q10 – Film le plus long par genre")
    st.write(q10_film_le_plus_long_par_genre())

    st.subheader(
        "Q11 – Vue des films avec Metascore > 80 et Revenue > 50M "
        "(affichés ici via une requête équivalente)"
    )
    st.write(q11_vue_films_bons_scores())

    st.subheader("Q12 – Corrélation durée (Runtime) / revenu (Revenue)")
    corr = q12_correlation_runtime_revenue()
    st.write({"correlation_runtime_revenue": corr})

    st.subheader("Q13 – Évolution de la durée moyenne des films par décennie")
    st.write(q13_evolution_duree_moyenne_par_decennie())

# --------------------------------------------------------------------
# MONGO – VISUALISATIONS
# --------------------------------------------------------------------
elif section == "Mongo – Visualisations":
    st.header("MongoDB – Visualisations")

    st.subheader("Q4 – Histogramme : nombre de films par année")
    fig = plot_hist_films_par_annee()
    st.pyplot(fig)

    st.subheader("Q12 – Nuage de points durée / revenus")
    fig2 = plot_scatter_runtime_revenue()
    st.pyplot(fig2)

# --------------------------------------------------------------------
# NEO4J – QUESTIONS 14–22 (sans paramètres compliqués)
# --------------------------------------------------------------------
elif section == "Neo4j – Questions 14–22":
    st.header("Neo4j – Questions 14 à 22")

    st.subheader("Q14 – Acteur ayant joué dans le plus grand nombre de films")
    st.write(q14_acteur_plus_de_films())

    st.subheader(
        "Q15 – Acteurs ayant joué dans des films où Anne Hathaway a également joué"
    )
    st.write(q15_acteurs_ayant_joue_avec_anne_hathaway())

    st.subheader("Q16 – Acteur ayant joué dans des films totalisant le plus de revenus")
    st.write(q16_acteur_revenus_max())

    st.subheader("Q17 – Moyenne des votes (tous films)")
    st.write(q17_moyenne_votes())

    st.subheader("Q18 – Genre le plus représenté dans la base")
    st.write(q18_genre_le_plus_represente())

    st.subheader(
        "Q19 – Films dans lesquels les acteurs ayant joué avec vous ont également joué"
    )
    nom_vous = st.text_input(
        "Votre nom d’acteur dans le graphe (Q19)", value="Scarlett Johansson"
    )
    if nom_vous:
        st.write(q19_films_avec_acteurs_ayant_joue_avec_vous(nom_vous))

    st.subheader(
        "Q20 – Réalisateur ayant travaillé avec le plus grand nombre d’acteurs distincts"
    )
    st.write(q20_realisateur_plus_d_acteurs_distincts())

    st.subheader(
        "Q21 – Films les plus connectés (ayant le plus d’acteurs en commun avec d’autres films)"
    )
    st.write(q21_films_les_plus_connectes())

    st.subheader(
        "Q22 – Top 5 acteurs ayant joué avec le plus de réalisateurs différents"
    )
    st.write(q22_top5_acteurs_plus_de_realisateurs())

# --------------------------------------------------------------------
# NEO4J – AVANCÉ 23–30 (reco, influence, communautés, etc.)
# --------------------------------------------------------------------
elif section == "Neo4j – Avancé 23–30":
    st.header("Neo4j – Questions avancées 23 à 30")

    # Q23 – Recommander un film à un acteur
    st.subheader(
        "Q23 – Recommander un film à un acteur en fonction des genres des films "
        "où il a déjà joué"
    )
    acteur_q23 = st.text_input(
        "Nom de l’acteur pour la recommandation (Q23)", value="Tom Hanks"
    )
    if st.button("Lancer la recommandation (Q23)"):
        st.write(q23_recommande_films_pour_acteur(acteur_q23))

    # Q24 – Créer les relations INFLUENCE_PAR entre réalisateurs
    st.subheader(
        "Q24 – Créer des relations INFLUENCE_PAR entre réalisateurs "
        "(similarité de genres)"
    )
    if st.button("Créer/mettre à jour les relations INFLUENCE_PAR (Q24)"):
        st.write(q24_creer_relations_influence_par())

    # Q25 – Chemin le plus court entre deux acteurs
    st.subheader(
        "Q25 – Chemin le plus court entre deux acteurs (ex : Tom Hanks et Scarlett Johansson)"
    )
    acteur1 = st.text_input("Acteur 1 (Q25)", value="Tom Hanks")
    acteur2 = st.text_input("Acteur 2 (Q25)", value="Scarlett Johansson")
    if st.button("Trouver le chemin le plus court (Q25)"):
        st.write(chemin_plus_court_entre_acteurs(acteur1, acteur2))

    # Q26 – Communautés d’acteurs (Louvain)
    st.subheader(
        "Q26 – Analyser les communautés d’acteurs (Louvain – nécessite Neo4j GDS)"
    )
    if st.button("Calculer / mettre à jour les communautés (Q26)"):
        st.write(q26_calculer_communautes_acteurs())

    # Q27 – Films avec genres en commun mais réalisateurs différents
    st.subheader(
        "Q27 – Films qui ont des genres en commun mais des réalisateurs différents"
    )
    if st.button("Lister quelques films (Q27)"):
        st.write(q27_films_genres_communs_realisateurs_diff())

    # Q28 – Recommander des films aux utilisateurs selon les préférences d’un acteur
    st.subheader(
        "Q28 – Recommander des films aux utilisateurs en fonction des "
        "préférences d’un acteur"
    )
    acteur_q28 = st.text_input(
        "Acteur de référence pour la recommandation (Q28)", value="Scarlett Johansson"
    )
    if st.button("Recommander (Q28)"):
        st.write(q28_recommande_films_selon_preferences_acteur(acteur_q28))

    # Q29 – Créer des relations de concurrence entre réalisateurs
    st.subheader(
        "Q29 – Créer une relation de concurrence entre réalisateurs ayant "
        "fait des films similaires la même année"
    )
    if st.button("Créer/mettre à jour les relations CONCURRENCE (Q29)"):
        st.write(q29_creer_relations_concurrence())

    # Q30 – Collaborations fréquentes réalisateur–acteur et succès
    st.subheader(
        "Q30 – Collaborations fréquentes réalisateur–acteur et succès "
        "commercial / critique"
    )
    if st.button("Lister les collaborations fréquentes (Q30)"):
        st.write(q30_collaborations_frequentes_succes())
