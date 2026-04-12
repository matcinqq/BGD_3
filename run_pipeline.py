import argparse
import os
import platform
import shutil
import subprocess
from pathlib import Path
from urllib.request import urlretrieve
import zipfile


DEFAULT_DATASET_URL = "https://files.grouplens.org/datasets/movielens/ml-32m.zip"


def build_parser():
    parser = argparse.ArgumentParser(description="Run simple Spark medallion pipeline on MovieLens 32M.")
    parser.add_argument(
        "--project-root",
        default=str(Path(__file__).resolve().parent),
        help="Absolute path to project root.",
    )
    parser.add_argument(
        "--dataset-url",
        default=DEFAULT_DATASET_URL,
        help="MovieLens dataset URL.",
    )
    return parser


def _java_executable_for_home(java_home):
    bin_dir = Path(java_home) / "bin"
    java_candidates = [bin_dir / "java"]
    if platform.system() == "Windows":
        java_candidates.insert(0, bin_dir / "java.exe")

    for java_executable in java_candidates:
        if java_executable.exists():
            return java_executable
    return None


def _candidate_java_homes():
    system = platform.system()
    candidates = []

    if system == "Darwin":
        candidates.extend(
            [
                Path("/usr/local/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"),
                Path("/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home"),
            ]
        )
        jvm_root = Path("/Library/Java/JavaVirtualMachines")
        if jvm_root.exists():
            for jdk_dir in sorted(jvm_root.glob("*")):
                candidates.append(jdk_dir / "Contents" / "Home")
    elif system == "Windows":
        base_dirs = []
        for env_name in ("ProgramFiles", "ProgramFiles(x86)", "LOCALAPPDATA"):
            env_value = os.environ.get(env_name)
            if env_value:
                base_dirs.append(Path(env_value))

        for base_dir in base_dirs:
            candidates.extend(base_dir.glob("Java/jdk*"))
            candidates.extend(base_dir.glob("Eclipse Adoptium/jdk*"))
            candidates.extend(base_dir.glob("Microsoft/jdk*"))
    else:
        jvm_root = Path("/usr/lib/jvm")
        if jvm_root.exists():
            for jdk_dir in sorted(jvm_root.glob("*")):
                candidates.append(jdk_dir)

    def preference(path_obj):
        path_str = str(path_obj).lower()
        return (0 if "21" in path_str else 1, path_str)

    return sorted(candidates, key=preference)


def _java_home_from_java_command(java_executable):
    try:
        result = subprocess.run(
            [java_executable, "-XshowSettings:properties", "-version"],
            check=False,
            capture_output=True,
            text=True,
        )
    except Exception:
        return None

    output = f"{result.stdout}\n{result.stderr}"
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.startswith("java.home = "):
            home = stripped.split("=", 1)[1].strip()
            if home:
                return home
    return None


def configure_java_for_spark():
    current_java_home = os.environ.get("JAVA_HOME")
    if current_java_home:
        if _java_executable_for_home(current_java_home):
            return current_java_home

    for home in _candidate_java_homes():
        if _java_executable_for_home(home):
            os.environ["JAVA_HOME"] = str(home)
            os.environ["PATH"] = f"{home / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"
            return str(home)

    java_from_path = shutil.which("java")
    if java_from_path:
        java_home_guess = _java_home_from_java_command(java_from_path)
        if java_home_guess and _java_executable_for_home(java_home_guess):
            os.environ["JAVA_HOME"] = str(java_home_guess)
            os.environ["PATH"] = f"{Path(java_home_guess) / 'bin'}{os.pathsep}{os.environ.get('PATH', '')}"
            return str(java_home_guess)

    return None


def ensure_movielens_dataset(project_root, dataset_url):
    source_dir = project_root / "data" / "source"
    zip_path = source_dir / "ml-32m.zip"
    extract_dir = source_dir / "ml-32m"
    ratings_csv = extract_dir / "ratings.csv"
    movies_csv = extract_dir / "movies.csv"

    if ratings_csv.exists() and movies_csv.exists():
        return ratings_csv, movies_csv

    source_dir.mkdir(parents=True, exist_ok=True)
    if not zip_path.exists():
        urlretrieve(dataset_url, str(zip_path))

    extract_dir.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as archive:
        archive.extractall(extract_dir)

    nested_dir = extract_dir / "ml-32m"
    if nested_dir.exists():
        for file_name in ("ratings.csv", "movies.csv", "links.csv", "tags.csv", "README.txt"):
            source_file = nested_dir / file_name
            target_file = extract_dir / file_name
            if source_file.exists() and not target_file.exists():
                source_file.replace(target_file)

    if not ratings_csv.exists() or not movies_csv.exists():
        raise RuntimeError("MovieLens files were not found after extraction.")
    return ratings_csv, movies_csv


def run_spark_pipeline(project_root, ratings_csv, movies_csv):
    java_home = configure_java_for_spark()
    if java_home is None and "JAVA_HOME" not in os.environ:
        raise RuntimeError(
            "Java not found. Install OpenJDK 21+ and set JAVA_HOME "
            "(Windows: setx JAVA_HOME \"C:\\Program Files\\Java\\jdk-21\")."
        )

    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    medallion = project_root / "data" / "medallion"
    bronze_ratings_path = medallion / "bronze" / "ratings"
    bronze_movies_path = medallion / "bronze" / "movies"
    silver_ratings_path = medallion / "silver" / "ratings_enriched"
    gold_metrics_path = medallion / "gold" / "movie_daily_metrics"

    spark = (
        SparkSession.builder.master("local[2]")
        .appName("movielens-medallion-spark")
        .config("spark.driver.memory", "4g")
        .config("spark.executor.memory", "4g")
        .config("spark.default.parallelism", "4")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )

    try:
        source_ratings = (
            spark.read.option("header", True)
            .csv(str(ratings_csv))
            .select(
                F.col("userId").cast("long").alias("user_id"),
                F.col("movieId").cast("long").alias("movie_id"),
                F.col("rating").cast("double").alias("rating"),
                F.col("timestamp").cast("long").alias("rating_ts"),
            )
            .withColumn("rated_at", F.to_timestamp(F.from_unixtime("rating_ts")))
            .withColumn("ingested_at", F.current_timestamp())
        )

        source_movies = (
            spark.read.option("header", True)
            .csv(str(movies_csv))
            .select(
                F.col("movieId").cast("long").alias("movie_id"),
                F.col("title").alias("title"),
                F.col("genres").alias("genres"),
            )
            .withColumn("ingested_at", F.current_timestamp())
        )

        bronze_ratings = source_ratings.dropDuplicates(["user_id", "movie_id", "rating_ts"])
        bronze_movies = source_movies.dropDuplicates(["movie_id"])

        bronze_ratings.write.mode("overwrite").parquet(str(bronze_ratings_path))
        bronze_movies.write.mode("overwrite").parquet(str(bronze_movies_path))

        silver_ratings = (
            bronze_ratings.join(bronze_movies.select("movie_id", "title", "genres"), on="movie_id", how="left")
            .where((F.col("rating") >= F.lit(0.5)) & (F.col("rating") <= F.lit(5.0)))
            .withColumn("rating_date", F.to_date("rated_at"))
        )
        silver_ratings.write.mode("overwrite").parquet(str(silver_ratings_path))

        gold_metrics = silver_ratings.groupBy("rating_date", "movie_id", "title").agg(
            F.count(F.lit(1)).alias("ratings_count"),
            F.round(F.avg("rating"), 4).alias("avg_rating"),
            F.min("rating").alias("min_rating"),
            F.max("rating").alias("max_rating"),
        )
        gold_metrics.write.mode("overwrite").parquet(str(gold_metrics_path))

        return {
            "source_ratings_rows": source_ratings.count(),
            "source_movies_rows": source_movies.count(),
            "bronze_ratings_rows": bronze_ratings.count(),
            "bronze_movies_rows": bronze_movies.count(),
            "silver_rows": silver_ratings.count(),
            "gold_rows": gold_metrics.count(),
        }
    finally:
        try:
            spark.stop()
        except Exception:
            pass


def main():
    args = build_parser().parse_args()
    project_root = Path(args.project_root).resolve()

    ratings_csv, movies_csv = ensure_movielens_dataset(project_root, args.dataset_url)
    metrics = run_spark_pipeline(project_root, ratings_csv, movies_csv)
    summary = {"source": str(ratings_csv), "metrics": metrics}
    print(f"Pipeline summary: {summary}")


if __name__ == "__main__":
    main()
