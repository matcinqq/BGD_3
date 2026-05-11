# Data Product Card (MS Teams Marketplace)

## 1. What is the product

**Nazwa:** MovieLens Daily Rating Metrics  
**Typ:** Data Product (Gold layer w architekturze medallion)  
**Owner:** Marcin Hyla

Produkt dostarcza dzienne metryki ocen filmów (`count`, `avg`, `min`, `max`) liczone na bazie MovieLens 32M.

## 2. What problem it solves

Produkt odpowiada na pytania:
- które filmy mają najwyższą aktywność ocen w czasie,
- jak zmieniają się średnie oceny dziennie,
- które tytuły mają stabilne / niestabilne oceny.

## 3. Data source

- Source: MovieLens 32M
- URL: <https://files.grouplens.org/datasets/movielens/ml-32m.zip>
- Pliki wejściowe: `ratings.csv`, `movies.csv`

## 4. Access instructions

1. Sklonuj repozytorium.
2. Uruchom pipeline:
   - `python run_pipeline.py`
3. Odczytaj wynik:
   - `data/medallion/gold/movie_daily_metrics` (Parquet)

Jeśli repo jest prywatne, dostęp po zaproszeniu do GitHub.

## 5. Schema (gold layer)

| Column | Type | Description |
|---|---|---|
| `rating_date` | date | Dzień agregacji |
| `movie_id` | long | Id filmu |
| `title` | string | Tytuł filmu |
| `ratings_count` | long | Liczba ocen w danym dniu |
| `avg_rating` | double | Średnia ocena |
| `min_rating` | double | Minimalna ocena |
| `max_rating` | double | Maksymalna ocena |

## 6. Key quality metrics

Pomiar z runu: **2026-05-10**

- `dq_completeness_rating_pct`: `100.0%` (threshold `>= 99.0%`)
- `dq_uniqueness_movie_id_pct`: `100.0%` (threshold `= 100.0%`)
- `dq_valid_rating_range_pct`: `100.0%` (threshold `>= 99.0%`)
- `dq_freshness_hours`: `0.0h` (threshold `<= 24.0h`)

Szczegóły: `docs/quality_metrics.md`, `data_product_contract.yaml`.

## 7. Known limitations

- Pipeline działa lokalnie (Spark + Java), nie jest wdrożony jako centralny serwis.
- Zapis warstw jest w trybie overwrite.
- Brak pełnej historii wersji danych między uruchomieniami.

## 8. Example usage

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.master("local[2]").appName("read-gold").getOrCreate()
gold = spark.read.parquet("data/medallion/gold/movie_daily_metrics")
gold.orderBy("rating_date", ascending=False).show(20, truncate=False)
```

## 9. Contact

- Owner: Marcin Hyla
- Repo / contract: `data_product_contract.yaml`
