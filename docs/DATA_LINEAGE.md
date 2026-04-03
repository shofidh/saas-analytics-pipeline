# Data Lineage — SaaS Analytics Platform

## Lineage Overview

```
raw_data/                       MinIO                    ClickHouse
(local CSV)                 (Object Store)              (Analytical DB)
─────────────               ──────────────              ────────────────────────────────

accounts.csv    ──[ingest]──► accounts/                ──[transform]──► staging.stg_accounts
  source: CRM               year=YY/month=MM/day=DD               ──[load_wh]──► warehouse.dim_accounts
  + _ingestion_time                                                            └──► mart.customer_health_score
  + _source_system                                                                  mart.mrr_monthly (via join)
  + _batch_id

subscriptions.csv ──[ingest]──► subscriptions/         ──[transform]──► staging.stg_subscriptions
  source: TXN               year=YY/month=MM/day=DD               ──[load_wh]──► warehouse.fact_subscriptions
  + _ingestion_time          (Parquet + metadata)      + churn_flag derived     └──► mart.mrr_monthly
  + _source_system                                       reactivation derived        mart.churn_summary (join)
  + _batch_id                                            sequence derived             mart.customer_health_score
                                                         mrr/arr validated

churn_events.csv ──[ingest]──► churn_events/           ──[transform]──► staging.stg_churn_events
  source: TXN               year=YY/month=MM/day=DD               ──[load_wh]──► warehouse.fact_churn_events
  + metadata                                                                   └──► mart.churn_summary
                                                                                     mart.customer_health_score

feature_usage.csv ──[ingest]──► feature_usage/         ──[transform]──► staging.stg_feature_usage
  source: app_logs          year=YY/month=MM/day=DD               ──[load_wh]──► warehouse.fact_feature_usage
  + metadata                                                                   └──► mart.feature_usage_summary
                                                                                     mart.customer_health_score (30d)

support_tickets.csv ──[ingest]──► support_tickets/     ──[transform]──► staging.stg_support_tickets
  source: support           year=YY/month=MM/day=DD               ──[load_wh]──► warehouse.fact_support_tickets
  + metadata                                                                   └──► mart.customer_health_score
```

---

## Stage-by-Stage Lineage Table

| Stage | Input | Output | Script | Key Transformations |
|---|---|---|---|---|
| **Ingest** | `data/raw_data/*.csv` | `minio://saas-raw/{table}/year=.../` | `incremental_ingestion.py` | Filter by `ingestion_*` watermark, add `_ingestion_time`, `_source_system`, `_batch_id`, convert to Parquet |
| **Transform** | MinIO Parquet partitions | `staging.stg_*` | `transform_incremental.py` | Dedup by PK, dtype cast, churn_flag (gap>60d), reactivation_flag, subscription_sequence, mrr/arr validation, column alignment |
| **DQ Check** | `staging.stg_*` | Pass/Fail signal | `data_quality_checks.py` | null PK, duplicate PK, negative revenue, date order, date bounds, critical null checks |
| **Load Dim** | `staging.stg_accounts` | `warehouse.dim_accounts` | `load_warehouse_incremental.py` | `warehouse_*` watermark-filtered INSERT, _updated_at versioning |
| **Load Fact** | `staging.stg_subscriptions` | `warehouse.fact_subscriptions` | `load_warehouse_incremental.py` | FK: account_id, plan_tier, start_date |
| **Load Fact** | `staging.stg_churn_events` | `warehouse.fact_churn_events` | `load_warehouse_incremental.py` | FK: account_id |
| **Load Fact** | `staging.stg_feature_usage` | `warehouse.fact_feature_usage` | `load_warehouse_incremental.py` | FK: subscription_id |
| **Load Fact** | `staging.stg_support_tickets` | `warehouse.fact_support_tickets` | `load_warehouse_incremental.py` | FK: account_id |
| **Mart: MRR** | `fact_subscriptions`, `fact_churn_events` | `mart.mrr_monthly` | dbt (incremental) | GROUP BY year, month, plan_tier — active, new, churned counts + MRR/ARR |
| **Mart: Churn** | `fact_churn_events`, `fact_subscriptions` | `mart.churn_summary` | dbt (incremental) | GROUP BY year, month, reason_code — churn_rate % |
| **Mart: Usage** | `fact_feature_usage` | `mart.feature_usage_summary` | dbt (incremental) | GROUP BY year, month, feature_name |
| **Mart: Health** | All dims and facts | `mart.customer_health_score` | dbt (incremental) | Composite score, churn_risk label, partial refresh for affected accounts |

---

## Metadata Tracking

Every row in every layer carries lineage metadata:

| Column | Layer | Description |
|---|---|---|
| `_ingestion_time` | MinIO, Staging | ISO timestamp when ingested from CSV |
| `_source_system` | MinIO, Staging | `crm_system` / `transactional_system` / `application_logs` / `support_system` |
| `_batch_id` | MinIO, Staging | UUID per ingestion run |
| `_updated_at` | Warehouse | Epoch ms — used by ReplacingMergeTree for latest-version deduplication |
| `_refreshed_at` | Mart | DateTime of last mart refresh |

---

## Watermark State (`data/watermarks/watermarks.json`)

```json
{
  "ingestion_accounts":               "2024-01-28",
  "ingestion_subscriptions":          "2024-01-28",
  "ingestion_churn_events":           "2024-01-28",
  "ingestion_feature_usage":          "2024-01-28",
  "ingestion_support_tickets":        "2024-01-28",

  "transform_accounts":              "2024-01-28",
  "transform_subscriptions":         "2024-01-28",
  "transform_churn_events":          "2024-01-28",
  "transform_feature_usage":         "2024-01-28",
  "transform_support_tickets":       "2024-01-28",

  "warehouse_dim_accounts":          "2024-01-28",
  "warehouse_fact_subscriptions":    "2024-01-28",
  "warehouse_fact_churn_events":     "2024-01-28",
  "warehouse_fact_feature_usage":    "2024-01-28",
  "warehouse_fact_support_tickets":  "2024-01-28"
}
```

**Watermark namespaces**:
| Prefix | Owner Script | Scope |
|---|---|---|
| `ingestion_` | `incremental_ingestion.py` | CSV → MinIO |
| `transform_` | `transform_incremental.py` | MinIO → ClickHouse staging |
| `warehouse_` | `load_warehouse_incremental.py` | Staging → warehouse dims/facts |

Each namespace advances independently, enabling:
- Replay a single layer without re-running upstream
- Different progress rates if one layer fails
- Selective backfill per layer

---

## Re-processing / Backfill

To re-process a specific date range:

1. **Reset watermarks** for the layer(s) you want to backfill:
   ```python
   from watermark_tracker import reset_watermark
   reset_watermark("ingestion_subscriptions")
   reset_watermark("transform_subscriptions")
   reset_watermark("warehouse_fact_subscriptions")
   ```

2. **Run the pipeline** with an override date:
   ```bash
   python incremental_ingestion.py 2024-06-01
   python transform_incremental.py 2024-06-01
   python load_warehouse_incremental.py 2024-06-01
   ```

3. **Or use Airflow backfill**:
   ```bash
   airflow dags backfill saas_incremental_pipeline \
     --start-date 2024-06-01 --end-date 2024-06-30
   ```
