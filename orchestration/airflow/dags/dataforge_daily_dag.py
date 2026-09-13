"""Apache Airflow DAG: DataForge daily batch pipeline.

An alternative orchestrator to Step Functions, useful when you want a
single pane of glass across many pipelines or richer scheduling. Runs the
same lifecycle: extract -> transform -> quality -> load.

Demonstrates core Airflow concepts:
- **DAG**          the pipeline definition + schedule.
- **task/operator** each step is a PythonOperator (could be GlueJobOperator
                   / RedshiftDataOperator on MWAA).
- **XCom**         tasks pass small results (row counts) downstream.
- **retry**        per-task retries with exponential backoff.
- **dependencies** ``>>`` sets execution order.

Runs against MWAA (Amazon Managed Workflows for Apache Airflow) or a local
Docker Airflow (see ../docker-compose.yaml) so no AWS account is required
to study it.
"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

default_args = {
    "owner": "data-eng",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}


def _extract(**context):
    from dataforge.ingestion.database.extract import extract_full

    df = extract_full("transactions")
    # Push the row count to XCom for downstream visibility.
    context["ti"].xcom_push(key="rows_extracted", value=len(df))
    return len(df)


def _transform(**context):
    from dataforge.pipelines.local_batch_pipeline import run

    summary = run()
    context["ti"].xcom_push(key="silver_rows", value=summary.get("silver_transactions", 0))
    return summary


def _quality(**context):
    from dataforge.data_quality.engine import run_expectations
    from dataforge.data_quality.rules import transaction_rules
    from dataforge.ingestion.database.extract import extract_full

    results = run_expectations(extract_full("transactions"), transaction_rules())
    # A real DAG would branch/fail here on critical failures.
    return {"overall_passed": results.passed}


def _load(**context):
    from dataforge.pipelines.incremental_pipeline import run

    return run()


with DAG(
    dag_id="dataforge_daily",
    description="DataForge daily batch ETL (extract -> transform -> quality -> load)",
    schedule="0 3 * * *",              # 03:00 daily
    start_date=datetime(2025, 1, 1),
    catchup=False,
    default_args=default_args,
    tags=["dataforge", "batch"],
) as dag:
    extract = PythonOperator(task_id="extract", python_callable=_extract)
    transform = PythonOperator(task_id="transform", python_callable=_transform)
    quality = PythonOperator(task_id="data_quality", python_callable=_quality)
    load = PythonOperator(task_id="load", python_callable=_load)

    # Dependency chain.
    extract >> transform >> quality >> load
