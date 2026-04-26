MovieLens Spark Pipeline

Simple Spark medallion pipeline built on MovieLens 32M.

Dataset:
- https://files.grouplens.org/datasets/movielens/ml-32m.zip

What this pipeline does:
1. Checks java (needed by Spark).
2. Downloads MovieLens 32M to data/source/ if files are missing.
3. Runs 3 layers:
   - Bronze: typed raw tables (ratings, movies)
   - Silver: ratings joined with movie metadata + valid rating filter (0.5 to 5.0)
   - Gold: daily movie stats (count, avg, min, max)
4. Writes parquet outputs to data/medallion/.

Main files:
- run_pipeline.py - entry point
- src/movielens_pipeline/config.py - project paths and dataset URL
- src/movielens_pipeline/java_env.py -java detection 
- src/movielens_pipeline/dataset.py - download + extract
- src/movielens_pipeline/spark_pipeline.py - spark transformations
- bgd_diagram.png- architecture diagram

Requirements:
- Python 3.11+
- Java 21+ 
- PySpark

Run:
  - python run_pipeline.py
