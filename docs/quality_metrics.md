# Data Quality Metrics

Poniżej metryki jakości dla produktu danych `gold_movie_daily_metrics`.

Wartości `current value` pochodzą z uruchomienia pipeline z dnia **2026-05-10**.

| Metric name | Definition | Current value | Expected threshold | Update cadence |
|---|---|---:|---|---|
| `dq_completeness_rating_pct` | % rekordów w Bronze (`ratings`) z niepustym `rating` | `100.0%` | `>= 99.0%` | Every pipeline run |
| `dq_uniqueness_movie_id_pct` | % unikalnych `movie_id` w Bronze (`movies`) po deduplikacji | `100.0%` | `= 100.0%` | Every pipeline run |
| `dq_valid_rating_range_pct` | % rekordów w Bronze (`ratings`) z `rating` w zakresie `0.5-5.0` | `100.0%` | `>= 99.0%` | Every pipeline run |
| `dq_freshness_hours` | Godziny od ostatniego `ingested_at` do momentu pomiaru | `0.0h` | `<= 24.0h` | Every pipeline run |

## Dodatkowe metryki wolumenu (run metadata)

- `source_ratings_rows`: `32,000,204`
- `source_movies_rows`: `87,585`
- `silver_rows`: `32,000,204`
- `gold_rows`: `14,695,580`
- `queue_push_count`: `2`
- `queue_pop_count`: `2`
