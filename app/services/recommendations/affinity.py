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
        combined_affinity_scores = self.recommender.process_interactions(
            data['data'], previous_affinities
        )

        ordered_movies = self.recommender.calculate_movie_scores(
            data['movies'], combined_affinity_scores
        )

        return ordered_movies
