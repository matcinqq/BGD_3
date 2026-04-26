from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent
SRC_DIR = PROJECT_ROOT / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from movielens_pipeline.config import DEFAULT_DATASET_URL, build_paths
from movielens_pipeline.dataset import ensure_movielens_dataset
from movielens_pipeline.java_env import configure_java_for_spark
from movielens_pipeline.spark_pipeline import run_spark_medallion_pipeline


def main():
    paths = build_paths(PROJECT_ROOT)

    if configure_java_for_spark() is None:
        raise RuntimeError(
            "Java not found. Install OpenJDK 21+ and set JAVA_HOME "
            "(Windows: setx JAVA_HOME \"C:\\Program Files\\Java\\jdk-21\")."
        )

    ratings_csv, _ = ensure_movielens_dataset(paths, DEFAULT_DATASET_URL)
    metrics = run_spark_medallion_pipeline(paths)
    summary = {"source": str(ratings_csv), "metrics": metrics}
    print(f"Pipeline summary: {summary}")


if __name__ == "__main__":
    main()
