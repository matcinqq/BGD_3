import os
from datetime import datetime, timezone


def detect_trigger_context():
    # Airflow provides these vars automatically inside DAG tasks.
    airflow_dag_id = os.getenv("AIRFLOW_CTX_DAG_ID")
    airflow_run_id = os.getenv("AIRFLOW_CTX_DAG_RUN_ID") or os.getenv("AIRFLOW_CTX_RUN_ID")
    airflow_execution_date = os.getenv("AIRFLOW_CTX_EXECUTION_DATE")

    if airflow_dag_id:
        return {
            "trigger_source": "airflow",
            "run_id": airflow_run_id or "airflow-run",
            "scheduled_at": airflow_execution_date or datetime.now(timezone.utc).isoformat(),
            "dag_id": airflow_dag_id,
        }

    return {
        "trigger_source": os.getenv("PIPELINE_TRIGGER", "local-trigger"),
        "run_id": os.getenv("PIPELINE_RUN_ID", "local-run"),
        "scheduled_at": os.getenv("PIPELINE_SCHEDULED_AT", datetime.now(timezone.utc).isoformat()),
        "dag_id": None,
    }


def build_ingestion_event(paths, trigger_context):
    # Small metadata payload kept with each ingestion run.
    return {
        "ratings_path": str(paths["ratings_csv"]),
        "movies_path": str(paths["movies_csv"]),
        "trigger_source": trigger_context["trigger_source"],
        "run_id": trigger_context["run_id"],
        "scheduled_at": trigger_context["scheduled_at"],
    }
