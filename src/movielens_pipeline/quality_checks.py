class DataQualityError(Exception):
    pass


def check_source_files(ratings_csv, movies_csv):
    if not ratings_csv.exists():
        raise DataQualityError(f"Missing source file: {ratings_csv}")
    if not movies_csv.exists():
        raise DataQualityError(f"Missing source file: {movies_csv}")


def check_metrics(metrics):
    required_keys = (
        "source_ratings_rows",
        "source_movies_rows",
        "bronze_ratings_rows",
        "bronze_movies_rows",
        "silver_rows",
        "gold_rows",
        "queue_push_count",
        "queue_pop_count",
    )

    for key in required_keys:
        if key not in metrics:
            raise DataQualityError(f"Metric '{key}' is missing.")
        if metrics[key] <= 0:
            raise DataQualityError(f"Metric '{key}' should be > 0, got {metrics[key]}.")

    if metrics["silver_rows"] > metrics["bronze_ratings_rows"]:
        raise DataQualityError(
            "Silver row count is greater than bronze ratings count, which should not happen."
        )
