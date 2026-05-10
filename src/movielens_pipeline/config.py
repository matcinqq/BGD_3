from pathlib import Path

DEFAULT_DATASET_URL = "https://files.grouplens.org/datasets/movielens/ml-32m.zip"
APP_NAME = "movielens-medallion-spark"
MIN_VALID_RATING = 0.5
MAX_VALID_RATING = 5.0


def build_paths(project_root):
    source_dir = project_root / "data" / "source"
    dataset_dir = source_dir / "ml-32m"
    medallion_dir = project_root / "data" / "medallion"
    return {
        "project_root": project_root,
        "source_dir": source_dir,
        "zip_path": source_dir / "ml-32m.zip",
        "ratings_csv": dataset_dir / "ratings.csv",
        "movies_csv": dataset_dir / "movies.csv",
        "bronze_ratings_path": medallion_dir / "bronze" / "ratings",
        "bronze_movies_path": medallion_dir / "bronze" / "movies",
        "silver_ratings_path": medallion_dir / "silver" / "ratings_enriched",
        "gold_metrics_path": medallion_dir / "gold" / "movie_daily_metrics",
    }
