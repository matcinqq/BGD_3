import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from movielens_pipeline.config import APP_NAME, MAX_VALID_RATING, MIN_VALID_RATING


def build_spark_session():
    master = os.getenv("SPARK_MASTER", "local[2]")
    driver_memory = os.getenv("SPARK_DRIVER_MEMORY", "4g")
    executor_memory = os.getenv("SPARK_EXECUTOR_MEMORY", "4g")
    shuffle_partitions = os.getenv("SPARK_SHUFFLE_PARTITIONS", "8")

    return (
        SparkSession.builder.master(master)
        .appName(APP_NAME)
        .config("spark.driver.memory", driver_memory)
        .config("spark.executor.memory", executor_memory)
        .config("spark.sql.shuffle.partitions", shuffle_partitions)
        .getOrCreate()
    )


def read_source_tables(spark, paths):
    ratings = (
        spark.read.option("header", True)
        .csv(str(paths["ratings_csv"]))
        .select(
            F.col("userId").cast("long").alias("user_id"),
            F.col("movieId").cast("long").alias("movie_id"),
            F.col("rating").cast("double").alias("rating"),
            F.col("timestamp").cast("long").alias("rating_ts"),
        )
        .withColumn("rated_at", F.to_timestamp(F.from_unixtime("rating_ts")))
        .withColumn("ingested_at", F.current_timestamp())
    )

    movies = (
        spark.read.option("header", True)
        .csv(str(paths["movies_csv"]))
        .select(
            F.col("movieId").cast("long").alias("movie_id"),
            F.col("title").alias("title"),
            F.col("genres").alias("genres"),
        )
        .withColumn("ingested_at", F.current_timestamp())
    )

    return ratings, movies


def build_bronze_layer(source_ratings, source_movies):
    bronze_ratings = source_ratings
    bronze_movies = source_movies.dropDuplicates(["movie_id"])
    return bronze_ratings, bronze_movies


def build_silver_layer(bronze_ratings, bronze_movies):
    return (
        bronze_ratings.join(bronze_movies.select("movie_id", "title", "genres"), on="movie_id", how="left")
        .where((F.col("rating") >= F.lit(MIN_VALID_RATING)) & (F.col("rating") <= F.lit(MAX_VALID_RATING)))
        .withColumn("rating_date", F.to_date("rated_at"))
    )


def build_gold_layer(silver_ratings):
    return silver_ratings.groupBy("rating_date", "movie_id", "title").agg(
        F.count(F.lit(1)).alias("ratings_count"),
        F.round(F.avg("rating"), 4).alias("avg_rating"),
        F.min("rating").alias("min_rating"),
        F.max("rating").alias("max_rating"),
    )


def write_layers(paths, bronze_ratings, bronze_movies, silver_ratings, gold_metrics):
    bronze_ratings.write.mode("overwrite").parquet(str(paths["bronze_ratings_path"]))
    bronze_movies.write.mode("overwrite").parquet(str(paths["bronze_movies_path"]))
    silver_ratings.write.mode("overwrite").parquet(str(paths["silver_ratings_path"]))
    gold_metrics.write.mode("overwrite").parquet(str(paths["gold_metrics_path"]))


def collect_metrics(source_ratings, source_movies, bronze_ratings, bronze_movies, silver_ratings, gold_metrics):
    # Basic row counts per layer.
    source_ratings_rows = source_ratings.count()
    source_movies_rows = source_movies.count()
    bronze_ratings_rows = bronze_ratings.count()
    bronze_movies_rows = bronze_movies.count()
    silver_rows = silver_ratings.count()
    gold_rows = gold_metrics.count()

    non_null_rating_rows = bronze_ratings.where(F.col("rating").isNotNull()).count()
    valid_rating_rows = bronze_ratings.where(
        (F.col("rating") >= F.lit(MIN_VALID_RATING)) & (F.col("rating") <= F.lit(MAX_VALID_RATING))
    ).count()
    # movies are deduped in Bronze, so this should stay at 100%.
    unique_movie_ids = bronze_movies.select("movie_id").distinct().count()

    # Freshness based on latest ingested_at in Bronze.
    freshness_seconds = bronze_ratings.agg(
        (
            F.unix_timestamp(F.current_timestamp()) - F.unix_timestamp(F.max("ingested_at"))
        ).alias("freshness_seconds")
    ).first()["freshness_seconds"]
    freshness_hours = None if freshness_seconds is None else round(float(freshness_seconds) / 3600.0, 6)

    return {
        "source_ratings_rows": source_ratings_rows,
        "source_movies_rows": source_movies_rows,
        "bronze_ratings_rows": bronze_ratings_rows,
        "bronze_movies_rows": bronze_movies_rows,
        "silver_rows": silver_rows,
        "gold_rows": gold_rows,
        "dq_completeness_rating_pct": round((non_null_rating_rows / bronze_ratings_rows) * 100.0, 6),
        "dq_uniqueness_movie_id_pct": round((unique_movie_ids / bronze_movies_rows) * 100.0, 6),
        "dq_valid_rating_range_pct": round((valid_rating_rows / bronze_ratings_rows) * 100.0, 6),
        "dq_freshness_hours": freshness_hours,
    }
