# Pipeline MovieLens (Spark)

Prosty pipeline danych oparty o **MovieLens 32M** i architekturę **Bronze / Silver / Gold**.

## Co robi pipeline

| Warstwa | Co zawiera | Wynik |
|---|---|---|
| Bronze | Wczytane i typowane dane `ratings` + `movies` | Surowe dane w parquet |
| Silver | Join ocen z metadanymi filmów + filtr poprawnych ocen (0.5–5.0) | `ratings_enriched` |
| Gold | Agregacje dzienne per film (count, avg, min, max) | `movie_daily_metrics` |

## Struktura

- `run_pipeline.py` — entrypoint
- `src/movielens_pipeline/orchestrator.py` — orkiestracja kroków
- `src/movielens_pipeline/spark_pipeline.py` — transformacje Spark
- `src/movielens_pipeline/quality_checks.py` — podstawowe kontrole jakości
- `src/movielens_pipeline/dataset.py` — pobranie i rozpakowanie danych

## Uruchomienie

```bash
source .venv/bin/activate
python run_pipeline.py
```

## Wymagania

- Python 3.11+
- Java 21+
- PySpark

Dataset: <https://files.grouplens.org/datasets/movielens/ml-32m.zip>
