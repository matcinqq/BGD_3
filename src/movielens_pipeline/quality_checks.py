class DataQualityError(Exception):
    pass


def check_source_files(ratings_csv, movies_csv):
    if not ratings_csv.exists():
        raise DataQualityError(f"Missing source file: {ratings_csv}")
    if not movies_csv.exists():
        raise DataQualityError(f"Missing source file: {movies_csv}")


def check_metrics(metrics):
    # Metrics that must always be present in pipeline output.
    required_keys = (
        "source_ratings_rows",
        "source_movies_rows",
        "bronze_ratings_rows",
        "bronze_movies_rows",
        "silver_rows",
        "gold_rows",
        "queue_push_count",
        "queue_pop_count",
        "dq_completeness_rating_pct",
        "dq_uniqueness_movie_id_pct",
        "dq_valid_rating_range_pct",
        "dq_freshness_hours",
    )

    for key in required_keys:
        if key not in metrics:
            raise DataQualityError(f"Metric '{key}' is missing.")

    positive_count_metrics = (
        "source_ratings_rows",
        "source_movies_rows",
        "bronze_ratings_rows",
        "bronze_movies_rows",
        "silver_rows",
        "gold_rows",
        "queue_push_count",
        "queue_pop_count",
    )

    for key in positive_count_metrics:
        if metrics[key] <= 0:
            raise DataQualityError(f"Metric '{key}' should be > 0, got {metrics[key]}.")

    # Silver is filtered from Bronze ratings, so it cannot grow.
    if metrics["silver_rows"] > metrics["bronze_ratings_rows"]:
        raise DataQualityError(
            "Silver row count is greater than bronze ratings count, which should not happen."
        )

    if metrics["dq_completeness_rating_pct"] < 99.0:
        raise DataQualityError(
            "Completeness for rating is below threshold 99.0%."
        )

    if metrics["dq_uniqueness_movie_id_pct"] < 100.0:
        raise DataQualityError(
            "Uniqueness for movie_id in bronze movies should be 100.0% after dedup."
        )

    if metrics["dq_valid_rating_range_pct"] < 99.0:
        raise DataQualityError(
            "Validity for rating range is below threshold 99.0%."
        )

    if metrics["dq_freshness_hours"] is None or metrics["dq_freshness_hours"] > 24.0:
        raise DataQualityError(
            "Freshness is above threshold 24h or missing."
        )
