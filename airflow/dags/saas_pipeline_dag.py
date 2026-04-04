"""
saas_pipeline_dag.py
=====================
Airflow DAG — SaaS Analytics incremental pipeline.

Task dependency graph (6 tasks)
────────────────────────────────
ingest_raw_to_minio
    └── transform_to_staging
            └── data_quality_checks
                    └── load_warehouse
                            └── dbt_run_marts
                                    └── pipeline_complete

All tasks are idempotent — safe to re-run or backfill.
batch_date = logical_date (data for that day processed on logical_date + 1).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, date, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator

SCRIPTS_DIR = os.getenv("SCRIPTS_DIR", "/app")
sys.path.insert(0, SCRIPTS_DIR)

# ─────────────────────────────────────────────────────────────
# DEFAULT ARGS
# ─────────────────────────────────────────────────────────────
default_args = {
    "owner":            "data-engineering",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}


def _batch_date(context: dict) -> date:
    """batch_date = logical_date (yesterday's data for @daily schedule)."""
    return context["logical_date"].date()


# ─────────────────────────────────────────────────────────────
# PYTHON CALLABLES
# ─────────────────────────────────────────────────────────────

def task_ingest_raw_to_minio(**context) -> None:
    """Read new CSV rows (watermark-filtered), upload as Parquet to MinIO."""
    from incremental_ingestion import run_ingestion
    run_ingestion(_batch_date(context))


def task_transform_to_staging(**context) -> None:
    """Read new MinIO partitions, apply business logic, load to ClickHouse staging."""
    from transform_incremental import run_transform
    run_transform(_batch_date(context))


def task_quality_checks(**context) -> None:
    """Run 7 automated DQ checks against staging tables."""
    from data_quality_checks import run_quality_checks
    run_quality_checks(_batch_date(context))


def task_load_warehouse(**context) -> None:
    """Watermark-filtered INSERT from staging → warehouse dims and facts."""
    from load_warehouse_incremental import run_warehouse_load
    run_warehouse_load(_batch_date(context))


# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="saas_incremental_pipeline",
    description="Incremental SaaS pipeline: CSV → MinIO → staging → production → dbt marts",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 2),
    catchup=False,
    max_active_runs=1,
    tags=["saas", "incremental", "production"],
    doc_md="""
## SaaS Incremental Pipeline

**Schedule**: Daily @ midnight UTC

**Flow** (6 tasks):
```
ingest_raw_to_minio → transform_to_staging → data_quality_checks
    → load_warehouse → dbt_run_marts → pipeline_complete
```
(staging → production → mart)

**Re-run**: Safe — all tasks are idempotent (watermark + ReplacingMergeTree).

**Backfill**:
```bash
airflow dags backfill saas_incremental_pipeline \\
  --start-date 2024-01-01 --end-date 2024-12-31
```
""",
) as dag:

    # ── 1. Ingest raw CSVs → MinIO (partitioned Parquet) ─────────────────────
    ingest = PythonOperator(
        task_id="ingest_raw_to_minio",
        python_callable=task_ingest_raw_to_minio,
        doc_md="Filter CSV rows by ingestion watermark, convert to Parquet, upload to MinIO.",
    )

    # ── 2. Transform MinIO → ClickHouse staging ─────────────────────────────
    transform = PythonOperator(
        task_id="transform_to_staging",
        python_callable=task_transform_to_staging,
        doc_md="Read new MinIO partitions, apply business logic (churn/reactivation/MRR), load to staging.",
    )

    # ── 3. Data quality gate ─────────────────────────────────────────────────
    dq_checks = PythonOperator(
        task_id="data_quality_checks",
        python_callable=task_quality_checks,
        doc_md="7 automated checks: null PK, duplicates, negative revenue, date bounds, critical nulls.",
    )

    # ── 4. Promote staging → warehouse (dims + facts) ────────────────────────
    load_wh = PythonOperator(
        task_id="load_warehouse",
        python_callable=task_load_warehouse,
        doc_md="Watermark-filtered INSERT from staging → warehouse dims and facts (ReplacingMergeTree).",
    )

    # ── 5. Build data marts via dbt ──────────────────────────────────────────
    dbt_run = BashOperator(
        task_id="dbt_run_marts",
        bash_command=(
            "cd /opt/dbt && dbt run --select mart --profiles-dir ."
            " --log-path /tmp/dbt-logs --target-path /tmp/dbt-target"
        ),
        doc_md="Run dbt incremental models: mrr_monthly, churn_summary, feature_usage_summary, customer_health_score.",
    )

    # ── 6. Sentinel ──────────────────────────────────────────────────────────
    done = EmptyOperator(task_id="pipeline_complete")

    # ── Dependency graph ─────────────────────────────────────────────────────
    ingest >> transform >> dq_checks >> load_wh >> dbt_run >> done
