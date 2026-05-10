# Pipeline MovieLens (Spark)

Prosty pipeline danych oparty o **MovieLens 32M** i architekturę **Bronze / Silver / Gold**.

## Co robi pipeline

| Warstwa | Co zawiera | Wynik |
|---|---|---|
| Bronze | Wczytane i typowane dane `ratings` + `movies` | Surowe dane w parquet |
| Silver | Join ocen z metadanymi filmów + filtr poprawnych ocen (0.5–5.0) | `ratings_enriched` |
| Gold | Agregacje dzienne per film (count, avg, min, max) | `movie_daily_metrics` |

Przepływ danych jest uruchamiany automatycznie przez trigger (np. Airflow), a wejście/wyjście pipeline przechodzi przez kolejkę (`in` / `out`).

## Struktura

- `run_pipeline.py` — entrypoint
- `src/movielens_pipeline/orchestrator.py` — orkiestracja kroków
- `src/movielens_pipeline/spark_pipeline.py` — transformacje Spark
- `src/movielens_pipeline/quality_checks.py` — podstawowe kontrole jakości
- `src/movielens_pipeline/dataset.py` — pobranie i rozpakowanie danych
- `src/movielens_pipeline/trigger.py` — kontekst triggera (Airflow/local)
- `src/movielens_pipeline/queueing.py` — kolejka transportowa dla ruchu `in/out`
- `docs/pipeline_architecture.mmd` — diagram architektury

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
