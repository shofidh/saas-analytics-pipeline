# SaaS Analytics Platform — Pipeline Architecture

## Overview

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    SAAS ANALYTICS DATA PLATFORM                             │
│                                                                             │
│  SOURCE SYSTEMS          OBJECT STORE       PROCESSING       WAREHOUSE      │
│                                                                             │
│  ┌─────────────┐         ┌──────────────┐   ┌──────────┐    ┌───────────┐  │
│  │  CRM System │──────►  │              │   │          │    │           │  │
│  │  accounts   │         │    MinIO     │   │  Pandas  │    │ClickHouse │  │
│  │             │         │  (S3-compat) │──►│  ETL     │───►│ Warehouse │  │
│  │ Transact.   │──────►  │              │   │          │    │(Star      │  │
│  │  System     │         │  Partitioned │   │ Business │    │ Schema)   │  │
│  │  subs/churn │         │  Parquet:    │   │  Logic:  │    │           │  │
│  │             │         │  /table/     │   │ • churn  │    │ dims      │  │
│  │  App Logs   │──────►  │  year=YYYY/  │   │ • react. │    │ facts     │  │
│  │  feature    │         │  month=MM/   │   │ • MRR    │    │           │  │
│  │  usage      │         │  day=DD/     │   │ • seq    │    └─────┬─────┘  │
│  │             │         │              │   └──────────┘          │        │
│  │  Support    │──────►  │  Immutable   │                         │        │
│  │  System     │         │  raw data    │   ┌──────────┐    ┌─────▼─────┐  │
│  │  tickets    │         │              │   │  Data    │    │   Data    │  │
│  └─────────────┘         └──────────────┘   │ Quality  │    │   Mart    │  │
│                                             │ Checks   │    │           │  │
│  INGESTION                                  │          │    │ mrr       │  │
│  ┌─────────────────────────────────────┐    │ • null PK│    │ churn     │  │
│  │        incremental_ingestion.py     │    │ • no dup │    │ usage     │  │
│  │                                     │    │ • no neg │    │ health    │  │
│  │  watermark = last ingested date     │    │   revenue│    └─────┬─────┘  │
│  │  new_data = data > watermark        │    │ • date   │          │        │
│  │  Upload as partitioned Parquet      │    │   bounds │          │        │
│  └─────────────────────────────────────┘    └──────────┘    ┌─────▼─────┐  │
│                                                              │ Metabase  │  │
│  TRANSFORMATION                                              │  (BI)     │  │
│  ┌─────────────────────────────────────┐                    │           │  │
│  │    transform_incremental.py         │                    │ Dashboards│  │
│  │                                     │                    │ Reports   │  │
│  │  Read new MinIO partitions          │                    └───────────┘  │
│  │  Apply business logic (churn,       │                                   │
│  │  reactivation, MRR, sequence)       │                                   │
│  │  Insert to ClickHouse staging       │                                   │
│  └─────────────────────────────────────┘                                   │
│                                                                             │
│  ORCHESTRATION                                                              │
│  ┌─────────────────────────────────────────────────────────┐               │
│  │                  Apache Airflow (standalone)            │               │
│  │  @daily │ saas_incremental_pipeline DAG                │               │
│  │                                                         │               │
│  │  ingest_raw_to_minio → transform_to_staging →          │               │
│  │    data_quality_checks → load_warehouse →              │               │
│  │      dbt_run_marts → pipeline_complete                 │               │
│  └─────────────────────────────────────────────────────────┘               │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Layer-by-Layer Explanation

### Layer 1: Source Data (Local Raw Files)

| File | Source System | Date Column | Volume |
|---|---|---|---|
| `accounts.csv` | CRM System | `signup_date` | 10,000 rows |
| `subscriptions.csv` | Transactional System | `start_date` | ~100,000 rows |
| `churn_events.csv` | Transactional System | `churn_date` | ~10,000 rows |
| `feature_usage.csv` | Application Logs | `usage_date` | 500,000 rows |
| `support_tickets.csv` | Support System | `submitted_at` | 50,000 rows |

**Simulated date range**: January 2024 → December 2026

### Layer 2: Incremental Ingestion (`incremental_ingestion.py`)

**Responsibility**: CSV → MinIO (raw Parquet, no business logic applied)

**Logic**:
```python
batch_date      = today() - 1 day           # simulates "yesterday arrived"
watermark       = get_watermark(f"ingestion_{table}")
new_data        = df[ (date_col > watermark) AND (date_col <= batch_date) ]
add_metadata(new_data, _ingestion_time, _source_system, _batch_id)
upload_parquet_to_minio(new_data, partition=partition_date)
set_watermark(f"ingestion_{table}", batch_date)
```

**MinIO partition structure**:
```
saas-raw/
  accounts/year=2024/month=01/day=01/part-0.parquet
  subscriptions/year=2024/month=01/day=01/part-0.parquet
  churn_events/year=2024/month=01/day=01/part-0.parquet
  feature_usage/year=2024/month=01/day=01/part-0.parquet
  support_tickets/year=2024/month=01/day=01/part-0.parquet
```

**Metadata added to Parquet**:
- `_ingestion_time` — ISO timestamp of upload
- `_source_system` — crm_system / transactional_system / application_logs / support_system
- `_batch_id` — UUID per run

**Idempotency**: Re-uploading the same partition date overwrites the file (no duplicates).

### Layer 3: Incremental Transform (`transform_incremental.py`)

**Responsibility**: MinIO → ClickHouse staging (with business logic)

**Logic**:
```python
watermark = get_watermark(f"transform_{table}")
df = read_partitions_from_minio(table, from_date=watermark, to_date=batch_date)
df = enrich(df)    # domain-specific business logic
df = align_columns(df, staging_schema)
insert_to_staging(ch, staging_table, df)
set_watermark(f"transform_{table}", batch_date)
```

**Business Logic Applied**:

| Derived Field | Logic |
|---|---|
| `subscription_sequence` | `groupby('account_id').cumcount() + 1` after sort by start_date |
| `days_active` | `(end_date - start_date).days` |
| `churn_flag` | gap to next subscription > 60 days, OR last known subscription |
| `reactivation_flag` | gap from previous end_date > 60 days |
| `mrr_amount` | clipped to ≥ 0, validated |
| `arr_amount` | `mrr * 12` |

### Layer 4: Data Quality (`data_quality_checks.py`)

Runs AFTER transformation to gate the pipeline:

| Check | Severity | Action on Fail |
|---|---|---|
| Non-empty batch | FAIL | Stop pipeline |
| No null PK | FAIL | Stop pipeline |
| No duplicate PK | FAIL | Stop pipeline |
| No negative revenue | FAIL | Stop pipeline |
| Date order (start ≤ end) | FAIL | Stop pipeline |
| Date bounds (2024–2026) | WARNING | Log only, continue |
| No null critical cols | FAIL | Stop pipeline |

### Layer 5: ClickHouse Warehouse (`load_warehouse_incremental.py`)

**UPSERT Pattern** (ClickHouse-native):
```sql
-- Insert new rows; engine keeps latest _updated_at per PK
INSERT INTO warehouse.fact_subscriptions
SELECT *, epoch_ms() AS _updated_at
FROM staging.stg_subscriptions
WHERE start_date > '{watermark}' AND start_date <= '{load_date}'
```

**Load order** (FK dependency):
1. `dim_accounts` (no deps)
2. `fact_subscriptions` (needs dim_accounts, dim_plans)
3. `fact_churn_events` (needs dim_accounts)
4. `fact_feature_usage` (needs fact_subscriptions)
5. `fact_support_tickets` (needs dim_accounts)

**Late-arriving data**: Always insert with current epoch ms as `_updated_at`. ReplacingMergeTree keeps the latest version automatically.

### Layer 6: Data Mart (dbt incremental models)

| Mart Table | Grain | Pattern |
|---|---|---|
| `mart.mrr_monthly` | year × month × plan_tier | `delete+insert` for months with new data |
| `mart.churn_summary` | year × month × reason_code | `delete+insert` for months with new data |
| `mart.feature_usage_summary` | year × month × feature_name | `delete+insert` for months with new data |
| `mart.customer_health_score` | account_id (latest snapshot) | `delete+insert` for affected accounts |

### Layer 7: Orchestration (Airflow DAG)

**DAG**: `saas_incremental_pipeline`
**Schedule**: `@daily` (data for yesterday)
**Mode**: Airflow standalone (webserver + scheduler in one process)

```
ingest_raw_to_minio ──► transform_to_staging ──► data_quality_checks
                                                         │
                                                  load_warehouse
                                                         │
                                                  dbt_run_marts
                                                         │
                                                  pipeline_complete
```

---

## Incremental vs Full Refresh

| Aspect | This Pipeline (Incremental) | Full Refresh |
|---|---|---|
| Data read | Only new rows since watermark | Entire dataset |
| MinIO overhead | One partition per date per table | Entire bucket scan |
| ClickHouse inserts | Hundreds to thousands of rows | Millions |
| Mart refresh | Only affected month/accounts | All data |
| Safe to re-run | ✅ Yes (idempotent) | ✅ Yes (overwrite) |
| Late data handling | ✅ Re-run with historical date | ✅ Naturally included |
| Suitable for | Daily micro-batches | Initial backfill |

---

## Technology Stack

| Component | Technology | Purpose |
|---|---|---|
| Object Storage | MinIO | S3-compatible raw Parquet data lake |
| Transformation | Python + Pandas | Lightweight ETL for this dataset scale |
| Data Format | Apache Parquet (Snappy) | Columnar, compressed, partitioned |
| Warehouse | ClickHouse | Analytical query engine |
| Warehouse Pattern | ReplacingMergeTree | Idempotent UPSERT |
| Mart Modeling | dbt (dbt-clickhouse) | Incremental aggregation models |
| Orchestration | Apache Airflow (standalone) | DAG scheduling, retry, monitoring |
| BI / Reporting | Metabase | Self-service dashboards |
| State Management | JSON Watermark File | Per-table last-processed date |
| Containerisation | Docker + docker-compose | Local reproducible environment |
