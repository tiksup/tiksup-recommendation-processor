import json
from collections import defaultdict
from pyspark.sql import Row


class Recommender:
    def process_interactions(self, data, previous_affinities=None):
        affinity_scores = defaultdict(float, previous_affinities or {})

        for interaction in data:
            genre = interaction['interactions'].get('genre', [])
            protagonist = interaction['interactions'].get('protagonist', "")
            director = interaction['interactions'].get('director', "")
            watching_repeat = interaction.get('watching_repeat', 1)
            watching_time = interaction.get('watching_time', 0)

            interaction_weight = watching_repeat * (watching_time / 60)

            for g in genre:
                affinity_scores[g] += interaction_weight
            affinity_scores[protagonist] += interaction_weight
            affinity_scores[director] += interaction_weight

        return dict(affinity_scores)

    def calculate_movie_scores(self, movies, affinity_scores) -> list[Row]:
        scored_movies = []
        for movie in movies:
            score = sum(affinity_scores.get(attr, 0) for attr in movie['genre'])
            score += affinity_scores.get(movie['protagonist'], 0)
            score += affinity_scores.get(movie['director'], 0)
            scored_movies.append((movie, score))

        scored_movies.sort(key=lambda x: x[1], reverse=True)

        return [movie for movie, score in scored_movies]
