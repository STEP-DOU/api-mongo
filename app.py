# app.py

import streamlit as st
from config.config import MONGO_URI, NEO4J_URI, NEO4J_USER, NEO4J_PASSWORD

# MongoDB imports
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
    get_avg_runtime_by_decade,
    recommend_film_mongo
)

# Neo4j imports
from database.neo4j import (
    connect_neo4j,
    test_connection,
    get_all_films,
    get_all_directors,
    get_films_by_director,
    get_most_active_actor,
    get_actors_who_played_with,
    get_top_grossing_actor,
    get_average_votes,
    get_most_common_genre,
    get_films_played_by_coactors,
    get_all_actors,
    get_director_with_most_actors,
    get_most_connected_films,
    get_actors_with_most_directors,
    recommend_film_by_genre,
    create_influence_relationships,
    get_shortest_path_between_actors,
    create_actor_collaboration_edges,
    detect_actor_communities,
    get_films_with_common_genres_diff_directors,
    get_preferred_genres_for_actor,
    create_director_concurrence_relationships,
    get_frequent_collaborations_with_success
)

st.set_page_config(page_title="NoSQL Explorer", layout="wide")
st.title("Projet NoSQL – MongoDB & Neo4j Explorer")

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

    driver = connect_neo4j()

    if st.button("✅ Tester la connexion à Neo4j"):
        try:
            message = test_connection(driver)
            st.success(message)
        except Exception as e:
            st.error(f"Erreur de connexion : {e}")

    st.subheader("🎬 Lister les films présents dans Neo4j")
    films = get_all_films(driver)
    st.write(films)

    st.subheader("🎥 Lister les réalisateurs")
    directors = get_all_directors(driver)
    selected_director = st.selectbox("Choisir un réalisateur", directors)

    if selected_director:
        films_by_director = get_films_by_director(driver, selected_director)
        st.write(f"Films réalisés par **{selected_director}** :")
        st.write(films_by_director)

    st.subheader("🎭 Acteur ayant joué dans le plus de films")
    if st.button("Afficher l'acteur le plus actif"):
        actor_info = get_most_active_actor(driver)
        if actor_info:
            st.success(f"{actor_info['actor']} a joué dans {actor_info['nb_films']} films.")
        else:
            st.warning("Aucun acteur trouvé.")

    st.subheader("🤝 Acteurs ayant joué avec Anne Hathaway")
    if st.button("Afficher les acteurs ayant partagé un film avec Anne Hathaway"):
        co_actors = get_actors_who_played_with(driver, "Anne Hathaway")
        if co_actors:
            st.write(f"{len(co_actors)} acteur(s) trouvé(s) :")
            st.write(co_actors)
        else:
            st.warning("Aucun acteur trouvé ou Anne Hathaway absente du graphe.")

    st.subheader("💰 Acteur ayant généré le plus de revenus")
    if st.button("Afficher l'acteur le plus rentable"):
        actor = get_top_grossing_actor(driver)
        if actor:
            st.success(f"{actor['actor']} – {actor['total_revenue']:.2f} M$")
        else:
            st.warning("Aucun acteur avec revenus disponibles.")

    st.subheader("⭐ Moyenne des votes des films")
    if st.button("Afficher la moyenne des votes"):
        avg = get_average_votes(driver)
        if avg:
            st.success(f"Moyenne des votes : {avg['avg_votes']:.2f}")
        else:
            st.warning("Aucune donnée de votes trouvée.")

    st.subheader("🎬 Genre le plus représenté")
    if st.button("Afficher le genre le plus fréquent"):
        genre = get_most_common_genre(driver)
        if genre:
            st.success(f"Genre : {genre['genre']} – Nombre de films : {genre['nb_films']}")
        else:
            st.warning("Aucun genre trouvé dans la base.")

    st.subheader("🎞️ Films dans lesquels les co-acteurs ont joué")
    actors = get_all_actors(driver)
    selected_actor = st.selectbox("Choisir un acteur", actors)
    if st.button("Afficher les films joués par ses co-acteurs"):
        films = get_films_played_by_coactors(driver, selected_actor)
        if films:
            st.info(f"{len(films)} film(s) trouvés :")
            st.write(films)
        else:
            st.warning("Aucun film trouvé ou acteur inconnu.")

    st.subheader("🎬 Réalisateur ayant travaillé avec le plus d'acteurs distincts")
    if st.button("Afficher le réalisateur le plus collaboratif"):
        director = get_director_with_most_actors(driver)
        if director:
            st.success(f"{director['director']} – {director['nb_actors']} acteur(s) différents")
        else:
            st.warning("Aucun réalisateur ou acteur trouvé dans la base.")

    st.subheader("🎞️ Films avec le plus d'acteurs")
    if st.button("Afficher les films les plus connectés"):
        top_films = get_most_connected_films(driver)
        if top_films:
            for film in top_films:
                st.markdown(f"- **{film['title']}** : {film['actors']} acteurs")
        else:
            st.warning("Aucun film trouvé.")

    st.subheader("🎭 Top 5 des acteurs ayant travaillé avec le plus de réalisateurs différents")
    if st.button("Afficher les 5 acteurs les plus connectés aux réalisateurs"):
        top_actors = get_actors_with_most_directors(driver)
        if top_actors:
            for a in top_actors:
                st.markdown(f"- **{a['actor']}** : {a['directors']} réalisateurs")
        else:
            st.warning("Aucun résultat.")

    st.subheader("🎯 Recommander un film à un acteur selon ses genres préférés")
    actor_for_reco = st.selectbox("Choisir un acteur pour la recommandation", actors)

    if st.button("Recommander un film"):
        reco = recommend_film_by_genre(driver, actor_for_reco)
        if reco:
            st.success(f"Film recommandé pour **{actor_for_reco}** : *{reco['title']}* (Genre : {reco['genre']})")
        else:
            st.warning("Aucune recommandation trouvée (acteur trop spécialisé ou tous les films déjà vus).")

    st.subheader("🔁 Relations d'influence entre réalisateurs")
    if st.button("Créer les relations :INFLUENCE_PAR"):
        msg = create_influence_relationships(driver)
        st.success(msg)

    st.subheader("🧭 Chemin le plus court entre deux acteurs")

    actor_a = st.selectbox("Acteur de départ", actors, key="actor_a")
    actor_b = st.selectbox("Acteur d'arrivée", actors, key="actor_b")

    if st.button("Trouver le plus court chemin entre ces deux acteurs"):
        if actor_a == actor_b:
            st.warning("Sélectionne deux acteurs différents.")
        else:
            path = get_shortest_path_between_actors(driver, actor_a, actor_b)
            if path:
                st.info(f"Chemin le plus court entre **{actor_a}** et **{actor_b}** :")
                st.write(" ➡️ ".join(path))
            else:
                st.error("Aucun chemin trouvé entre ces deux acteurs.")

    st.subheader("🧠 Détection des communautés d'acteurs (Louvain)")

    if st.button("Créer les relations A_JOUE_AVEC"):
        msg = create_actor_collaboration_edges(driver)
        st.success(msg)

    if st.button("Lancer la détection des communautés avec Louvain"):
        result = detect_actor_communities(driver)
        if result:
            current_community = None
            for r in result:
                if r['communityId'] != current_community:
                    current_community = r['communityId']
                    st.markdown(f"### 🎯 Communauté {current_community}")
                st.markdown(f"- {r['actor']}")
        else:
            st.warning("Aucune communauté détectée ou erreur GDS.")


# --- Analyse croisée ---
elif section == "Analyse croisée":
    st.header("🔄 Analyse croisée MongoDB & Neo4j")

    driver = connect_neo4j()
    from database.neo4j import get_films_with_common_genres_diff_directors

    st.subheader("🎬 Films avec genres en commun mais réalisateurs différents (27)")

    if st.button("Afficher les correspondances"):
        results = get_films_with_common_genres_diff_directors(driver)
        if results:
            for r in results:
                st.markdown(
                    f"- **{r['film1']}** (*{r['director1']}*) & **{r['film2']}** (*{r['director2']}*) – Genre commun : _{r['genre']}_"
                )
        else:
            st.warning("Aucune correspondance trouvée.")

    from database.mongo import recommend_film_mongo
    from database.neo4j import get_preferred_genres_for_actor
    mongo_client = connect_mongo(MONGO_URI)
    collection = mongo_client["entertainment"]["films"]

    st.subheader("🍿 Recommandation intelligente croisée (Neo4j + MongoDB) (28)")

    # Sélection de l'acteur
    selected_actor = st.selectbox("Choisir un acteur", get_all_actors(driver))

    if st.button("Recommander un film à cet acteur"):
        genres = get_preferred_genres_for_actor(driver, selected_actor)
        if genres:
            st.markdown(f"Génération d'une recommandation basée sur les genres préférés : {', '.join(genres)}")
            film = recommend_film_mongo(collection, genres, selected_actor)
            if film:
                st.success(f"🎬 Titre : **{film['title']}**")
                st.markdown(f"- 🎭 Genres : {film['genre']}")
                st.markdown(f"- ⭐ Note : {film['rating']}")
                st.markdown(f"- 👥 Votes : {film['Votes']}")
                st.markdown(f"Critères utilisés : {film['criteria']}")
            else:
                st.warning("Aucune recommandation trouvée avec ces critères.")
        else:
            st.warning("Genres préférés introuvables pour cet acteur.")

    from database.neo4j import create_director_concurrence_relationships

    st.subheader("⚔️ Relations de concurrence entre réalisateurs (29)")

    if st.button("Créer les relations :CONCURRENCE entre réalisateurs"):
        msg = create_director_concurrence_relationships(driver)
        st.success(msg)


    from database.neo4j import get_frequent_collaborations_with_success

    st.subheader("🎬 Collaborations fréquentes entre acteurs et réalisateurs avec succès (30)")

    if st.button("Afficher les collaborations fréquentes avec succès"):
        collaborations = get_frequent_collaborations_with_success(driver)
        if collaborations:
            for collab in collaborations:
                st.markdown(
                   f"- **{collab['actor']}** & **{collab['director']}** : {collab['collaborations']} collaborations – "
                   f"Revenu moyen : {collab['avg_revenue'] if collab['avg_revenue'] is not None else 'N/A'}M$ – "
                   f"Votes moyens : {collab['avg_votes'] if collab['avg_votes'] is not None else 'N/A'}"
)

        else:
            st.warning("Aucune collaboration fréquente trouvée.")
