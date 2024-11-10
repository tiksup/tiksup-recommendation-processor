from pyspark.sql import SparkSession
from .recommender import Recommender
from os import getenv


class Affinity:
    def __init__(self, recommender: Recommender) -> None:
        self.recommender = recommender

    def create_session(self) -> None:
        self.spark = SparkSession.builder \
                    .appName("MovieRecommendation") \
                    .master(f"spark://{getenv('SPARK_HOST')}:{getenv('SPARK_PORT')}") \
                    .getOrCreate()

    def process(self, data, previous_affinities) -> list[dict[str, any]]:
        movies_df = self.spark.createDataFrame(data['movies'])

        combined_affinity_scores = self.recommender.process_interactions(data['data'], previous_affinities)

        affinity_scores_df = self.spark.createDataFrame(
            [{"attribute": k, "score": v} for k, v in combined_affinity_scores.items()]
        )

        ordered_movies = self.recommender.calculate_movie_scores(movies_df, affinity_scores_df)

        result = [
            {
                "id": movie["id"],
                "url": movie["url"],
                "title": movie["title"],
                "genre": movie["genre"],
                "protagonist": movie["protagonist"],
                "director": movie["director"]
            } for movie in ordered_movies
        ]

        return result