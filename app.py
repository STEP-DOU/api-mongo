# app.py

import streamlit as st
from config.config import MONGO_URI
from database.mongo import (
    connect_mongo,
    get_most_common_year,
    count_movies_after_1999,
    average_votes_2007,
    get_films_per_year,
    get_genres,
    get_top_revenue_film,
    get_directors_with_more_than_5_films,
    get_best_avg_revenue_by_genre,
    get_top_rated_per_decade,
    get_longest_film_per_genre,
    create_high_score_view,
    compute_runtime_revenue_correlation,
    get_avg_runtime_by_decade
)

st.set_page_config(page_title="NoSQL Explorer", layout="wide")
st.title("🧠 Projet NoSQL – MongoDB & Neo4j Explorer")

section = st.sidebar.radio("📂 Choisir une base", ["MongoDB", "Neo4j", "Analyse croisée"])

# --- MongoDB Section ---
if section == "MongoDB":
    st.header("📦 Exploration de la base MongoDB")
    
    mongo_client = connect_mongo(MONGO_URI)
    db = mongo_client["entertainment"]
    collection = db["films"]
    
    st.subheader("🎯 Requêtes MongoDB")

    if st.button("📅 Année avec le plus de films"):
        result = get_most_common_year(collection)
        st.success(f"Année : {result['_id']} avec {result['count']} films.")

    if st.button("🎬 Nombre de films après 1999"):
        count = count_movies_after_1999(collection)
        st.info(f"Nombre de films sortis après 1999 : {count}")

    if st.button("⭐ Moyenne des votes en 2007"):
        avg = average_votes_2007(collection)
        st.info(f"Moyenne des votes (2007) : {avg:.2f}")

    if st.button("📈 Histogramme des films par année"):
        data = get_films_per_year(collection)
        st.bar_chart({d['_id']: d['count'] for d in data})

    if st.button("🎭 Genres de films disponibles"):
        genres = get_genres(collection)
        st.write(genres)

    if st.button("💰 Film ayant généré le plus de revenus"):
        film = get_top_revenue_film(collection)
        if film:
            st.write(film)
        else:
            st.warning("Aucun film avec revenu renseigné.")

    if st.button("🎬 Réalisateurs avec plus de 5 films"):
        directors = get_directors_with_more_than_5_films(collection)
        st.write(directors)

    if st.button("🏆 Genre rapportant le plus en moyenne"):
        genre = get_best_avg_revenue_by_genre(collection)
        if genre:
            st.success(f"Genre : {genre['_id'].strip()} – Revenu moyen : {genre['avgRevenue']:.2f} M$")
        else:
            st.warning("Aucun genre trouvé avec revenus valides.")

    if st.button("🎖️ Top 3 films par décennie (rating)"):
        data = get_top_rated_per_decade(collection)
        for d in data:
            st.markdown(f"**{d['_id']}** :")
            for film in d['top3']:
                title = film.get('title', 'Titre inconnu')
                rating = film.get('rating', 'Non classé')
                st.markdown(f"- {title} ({rating})")

    if st.button("⏱️ Film le plus long par genre"):
        data = get_longest_film_per_genre(collection)
        for d in data:
            st.markdown(f"**{d['_id'].strip()}** : {d['title']} ({d['runtime']} min)")

    if st.button("🔍 Créer la vue MongoDB (score > 80, revenu > 50M)"):
        msg = create_high_score_view(collection)
        st.success(msg)

    if st.button("📊 Corrélation durée / revenu"):
        corr = compute_runtime_revenue_correlation(collection)
        if corr is not None:
            st.info(f"Corrélation (runtime vs revenue) : {corr:.3f}")
        else:
            st.warning("Pas assez de données pour calculer la corrélation.")

    if st.button("📉 Durée moyenne des films par décennie"):
        data = get_avg_runtime_by_decade(collection)
        decades = [d['_id'] for d in data]
        avg_runtime = [d['avgRuntime'] for d in data]
        st.line_chart(dict(zip(decades, avg_runtime)))

# --- Neo4j Section ---
elif section == "Neo4j":
    st.header("🔗 Exploration de la base Neo4j")
    st.info("Les fonctionnalités Neo4j seront ajoutées prochainement.")

# --- Analyse croisée ---
elif section == "Analyse croisée":
    st.header("📊 Analyse croisée MongoDB & Neo4j")
    st.warning("Module en cours de développement.")
