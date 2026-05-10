from movielens_pipeline.config import DEFAULT_DATASET_URL, build_paths
from movielens_pipeline.dataset import ensure_movielens_dataset
from movielens_pipeline.java_env import configure_java_for_spark
from movielens_pipeline.queueing import PipelineQueue
from movielens_pipeline.quality_checks import check_metrics, check_source_files
from movielens_pipeline.spark_pipeline import (
    build_bronze_layer,
    build_gold_layer,
    build_silver_layer,
    build_spark_session,
    collect_metrics,
    read_source_tables,
    write_layers,
)
from movielens_pipeline.trigger import build_ingestion_event, detect_trigger_context


def run_project_pipeline(project_root):
    paths = build_paths(project_root)
    trigger_context = detect_trigger_context()
    pipeline_queue = PipelineQueue()

    if configure_java_for_spark() is None:
        raise RuntimeError(
            "Java not found. Install OpenJDK 21+ and set JAVA_HOME "
            "(Windows: setx JAVA_HOME \"C:\\Program Files\\Java\\jdk-21\")."
        )

    ratings_csv, movies_csv = ensure_movielens_dataset(paths, DEFAULT_DATASET_URL)
    check_source_files(ratings_csv, movies_csv)

    spark = build_spark_session()
    try:
        source_ratings, source_movies = read_source_tables(spark, paths)
        # IN queue: source payload enters processing flow.
        pipeline_queue.push(
            "in",
            {
                "ratings": source_ratings,
                "movies": source_movies,
                "ingestion_event": build_ingestion_event(paths, trigger_context),
            },
        )
        inbound_payload = pipeline_queue.pop("in")

        bronze_ratings, bronze_movies = build_bronze_layer(
            inbound_payload["ratings"], inbound_payload["movies"]
        )
        silver_ratings = build_silver_layer(bronze_ratings, bronze_movies)
        gold_metrics = build_gold_layer(silver_ratings)

        # OUT queue: processed payload goes to sink/write stage.
        pipeline_queue.push(
            "out",
            {
                "bronze_ratings": bronze_ratings,
                "bronze_movies": bronze_movies,
                "silver_ratings": silver_ratings,
                "gold_metrics": gold_metrics,
            },
        )
        outbound_payload = pipeline_queue.pop("out")
        write_layers(
            paths,
            outbound_payload["bronze_ratings"],
            outbound_payload["bronze_movies"],
            outbound_payload["silver_ratings"],
            outbound_payload["gold_metrics"],
        )
        metrics = collect_metrics(
            source_ratings,
            source_movies,
            bronze_ratings,
            bronze_movies,
            silver_ratings,
            gold_metrics,
        )
        metrics.update(pipeline_queue.metrics())
        check_metrics(metrics)
        return {
            "source": str(ratings_csv),
            "trigger": trigger_context,
            "metrics": metrics,
        }
    finally:
        try:
            spark.stop()
        except Exception:
            pass
