import json
from collections import defaultdict
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from os import getenv

input_data = {
    'user': '70d92803-624a-4f82-91ab-1f78c4954454',
    'data': [
        {
            'movie_id': '672c6102f16eeccce1964033', 
            'watching_time': 45.5, 
            'watching_repeat': 2, 
            'interactions': {
                'genre': ['Action', 'Adventure'], 
                'protagonist': 'Arnold Schwarzenegger', 
                'director': 'James Cameron'
            }
        },
        {
            'movie_id': '672c6102f16eeccce1964033', 
            'watching_time': 12.3, 
            'watching_repeat': 1, 
            'interactions': {
                'genre': ['Action', 'Adventure'], 
                'protagonist': 'Arnold Schwarzenegger', 
                'director': 'James Cameron'
            }
        },
        {
            'movie_id': '672c6102f16eeccce1964033', 
            'watching_time': 45.5, 
            'watching_repeat': 2, 
            'interactions': {
                'genre': ["Sci-Fi", "Action"], 
                'protagonist': 'Will Smith', 
                'director': 'Alex Proyas'
            }
        },
    ],
    'movies': [
        {
            "id":"67246102f16eeccce1964033",
            "url": "https://swsjrkitthcnpugjevou.supabase.co/storage/v1/object/public/movies/Marte.mp4",
            "title": "The Martian",
            "genre": ["Sci-Fi", "Adventure"],
            "protagonist": "Matt Damon",
            "director": "Ridley Scott"
        },
        {
            "id":"272c6102f16eeccce1964033",
            "url": "https://swsjrkitthcnpugjevou.supabase.co/storage/v1/object/public/movies/Terminator_3.mp4",
            "title": "Terminator 3: Rise of the Machines",
            "genre": ["Sci-Fi", "Action"],
            "protagonist": "Arnold Schwarzenegger",
            "director": "Jonathan Mostow"
        },
        {
            "id":"672c6102f16eeccce1964833",
            "url": "https://swsjrkitthcnpugjevou.supabase.co/storage/v1/object/public/movies/VOLVER_AL_FUTURO.mp4",
            "title": "Back to the Future",
            "genre": ["Sci-Fi", "Adventure"],
            "protagonist": "Michael J. Fox",
            "director": "Robert Zemeckis"
        },
        {
            "id":"672c6102786eeccce1964033",
            "url": "https://swsjrkitthcnpugjevou.supabase.co/storage/v1/object/public/movies/Distrito_9_Legendado.mp4",
            "title": "District 9",
            "genre": ["Sci-Fi", "Thriller"],
            "protagonist": "Sharlto Copley",
            "director": "Neill Blomkamp"
        },
        {
            "id":"672c6102f16eecc7e1964033",
            "url": "https://swsjrkitthcnpugjevou.supabase.co/storage/v1/object/public/movies/Ghost_in_the_Shell.mp4",
            "title": "Ghost in the Shell",
            "genre": ["Sci-Fi", "Action"],
            "protagonist": "Scarlett Johansson",
            "director": "Rupert Sanders"
        },
        {
            "id":"672c6106f16eeccce1964033",
            "url": "https://swsjrkitthcnpugjevou.supabase.co/storage/v1/object/public/movies/Matrix.mp4",
            "title": "The Matrix",
            "genre": ["Sci-Fi", "Action"],
            "protagonist": "Keanu Reeves",
            "director": "Lana Wachowski, Lilly Wachowski"
        },
        {
            "id":"672c6102f16eeccce1964013",
            "url": "https://swsjrkitthcnpugjevou.supabase.co/storage/v1/object/public/movies/The_Abyss_2.mp4",
            "title": "The Abyss 2",
            "genre": ["Sci-Fi", "Adventure"],
            "protagonist": "Ed Harris",
            "director": "James Cameron"
        },
        {
            "id":"672c6102f16eeccce1964033",
            "url": "https://swsjrkitthcnpugjevou.supabase.co/storage/v1/object/public/movies/Yo_robot.mp4",
            "title": "I, Robot",
            "genre": ["Sci-Fi", "Action"],
            "protagonist": "Will Smith",
            "director": "Alex Proyas"
        }
    ]
}


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

    def calculate_movie_scores(self, movies_df, affinity_scores_df):
        exploded_movies_df = movies_df.withColumn("genre_exploded", F.explode("genre"))

        scored_movies_df = exploded_movies_df \
            .join(affinity_scores_df, exploded_movies_df["genre_exploded"] == affinity_scores_df["attribute"], "left_outer") \
            .withColumn("genre_score", F.when(F.col("score").isNull(), 0).otherwise(F.col("score"))) \
            .drop("score", "attribute") \
            .join(affinity_scores_df, exploded_movies_df["protagonist"] == affinity_scores_df["attribute"], "left_outer") \
            .withColumn("protagonist_score", F.when(F.col("score").isNull(), 0).otherwise(F.col("score"))) \
            .drop("score", "attribute") \
            .join(affinity_scores_df, exploded_movies_df["director"] == affinity_scores_df["attribute"], "left_outer") \
            .withColumn("director_score", F.when(F.col("score").isNull(), 0).otherwise(F.col("score"))) \
            .drop("score", "attribute")

        scored_movies_df = scored_movies_df.withColumn(
            "total_score",
            scored_movies_df["genre_score"] + scored_movies_df["protagonist_score"] + scored_movies_df["director_score"]
        )
        scored_movies_df = scored_movies_df.dropDuplicates(["id"])
        final_movies_df = scored_movies_df.orderBy("total_score", ascending=False)

        return final_movies_df.collect()


class Affinity(Recommender):
    def create_session(self):
        self.spark = SparkSession.builder\
                    .appName("MovieRecommendation") \
                    .master(f"spark://{getenv('SPARK_HOST')}:{getenv('SPARK_PORT')}") \
                    .getOrCreate()

    def process(self, data, previous_affinities):
        movies_df = self.spark.createDataFrame(data['movies'])

        combined_affinity_scores = self.process_interactions(data['data'], previous_affinities)

        affinity_scores_df = self.spark.createDataFrame(
            [{"attribute": k, "score": v} for k, v in combined_affinity_scores.items()]
        )

        ordered_movies = self.calculate_movie_scores(movies_df, affinity_scores_df)

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

        print(json.dumps(result, indent=2))


previous_affinities = {
    'Action': 10.5,
    'Adventure': 3.0,
    'Will Smith': 50.5,
    'Alex Proyas': 30.8
}

recommender = Affinity()
recommender.create_session()
recommender.process(input_data, previous_affinities)
