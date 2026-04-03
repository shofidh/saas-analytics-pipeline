# SaaS Analytics Platform — Entity Relationship Diagram (ERD)

## Data Model Overview

The schema follows a **Star Schema** design, normalized to 3NF at the dimension level:
- **3 Dimension tables** — slowly-changing reference data
- **4 Fact tables** — transactional events with FK references to dims
- **4 Mart tables** — pre-aggregated for BI (read-only, computed downstream)

---

## Entity Relationship Diagram (Text-based)

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         DIMENSION TABLES                                │
│                                                                         │
│  ┌─────────────────┐    ┌──────────────────┐    ┌─────────────────┐    │
│  │  dim_accounts   │    │    dim_plans     │    │    dim_date     │    │
│  │─────────────────│    │──────────────────│    │─────────────────│    │
│  │ PK account_id   │    │ PK plan_tier     │    │ PK date_key     │    │
│  │ account_name    │    │ price_per_seat   │    │ year            │    │
│  │ industry        │    │ billing_type     │    │ quarter         │    │
│  │ country         │    │ description      │    │ month           │    │
│  │ signup_date     │    │ _updated_at      │    │ month_name      │    │
│  │ referral_source │    └──────┬───────────┘    │ week_of_year    │    │
│  │ plan_tier       │           │                │ day_of_week     │    │
│  │ seats           │           │                │ day_name        │    │
│  │ is_trial        │           │                │ is_weekend      │    │
│  │ churn_flag      │           │                │ is_month_start  │    │
│  │ _source_system  │           │                │ is_month_end    │    │
│  │ _updated_at     │           │                └────────┬────────┘    │
│  └──────┬──────────┘           │                         │             │
│         │                      │                         │             │
└─────────│──────────────────────│─────────────────────────│─────────────┘
          │                      │                         │
          │ FK                   │ FK                      │ FK (start_date)
          ▼                      ▼                         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                           FACT TABLES                                   │
│                                                                         │
│  ┌──────────────────────────────────────────────────────────────────┐   │
│  │                    fact_subscriptions                            │   │
│  │──────────────────────────────────────────────────────────────────│   │
│  │ PK  subscription_id                                              │   │
│  │ FK  account_id        → dim_accounts.account_id                  │   │
│  │ FK  plan_tier         → dim_plans.plan_tier                      │   │
│  │ FK  start_date        → dim_date.date_key                        │   │
│  │     end_date                                                     │   │
│  │     seats, price, discount_value                                 │   │
│  │     mrr_amount, arr_amount                                       │   │
│  │     is_trial, upgrade_flag, downgrade_flag                       │   │
│  │     churn_flag, reactivation_flag                                │   │
│  │     billing_frequency, auto_renew_flag                           │   │
│  │     subscription_sequence  ← derived: rank within account        │   │
│  │     days_active             ← derived: end_date - start_date     │   │
│  │     _source_system, _updated_at                                  │   │
│  └──────────────────────────────────┬───────────────────────────────┘   │
│                                     │                                   │
│             ┌───────────────────────┘                                   │
│             │ FK (subscription_id)                                       │
│             ▼                                                            │
│  ┌───────────────────────┐   ┌──────────────────────┐                  │
│  │  fact_feature_usage   │   │   fact_churn_events  │                  │
│  │───────────────────────│   │──────────────────────│                  │
│  │ PK usage_id           │   │ PK churn_event_id    │                  │
│  │ FK subscription_id    │   │ FK account_id        │                  │
│  │ FK usage_date         │   │ FK churn_date        │                  │
│  │    feature_name       │   │    reason_code       │                  │
│  │    usage_count        │   │    refund_amount_usd │                  │
│  │    usage_duration_secs│   │    preceding_upgrade │                  │
│  │    error_count        │   │    preceding_downgrad│                  │
│  │    is_beta_feature    │   │    is_reactivation   │                  │
│  │    _source_system     │   │    feedback_text     │                  │
│  │    _updated_at        │   │    _source_system    │                  │
│  └───────────────────────┘   │    _updated_at       │                  │
│                              └──────────────────────┘                  │
│  ┌──────────────────────────────────────────────────┐                  │
│  │             fact_support_tickets                 │                  │
│  │──────────────────────────────────────────────────│                  │
│  │ PK  ticket_id                                    │                  │
│  │ FK  account_id        → dim_accounts.account_id  │                  │
│  │ FK  submitted_at      → dim_date.date_key        │                  │
│  │     closed_at                                    │                  │
│  │     resolution_time_hours                        │                  │
│  │     priority, first_response_time_minutes        │                  │
│  │     satisfaction_score, escalation_flag          │                  │
│  │     _source_system, _updated_at                  │                  │
│  └──────────────────────────────────────────────────┘                  │
└─────────────────────────────────────────────────────────────────────────┘

                              ▼ aggregated into ▼

┌─────────────────────────────────────────────────────────────────────────┐
│                           DATA MART TABLES                              │
│                                                                         │
│  mart.mrr_monthly          grain: year × month × plan_tier             │
│  mart.churn_summary        grain: year × month × reason_code           │
│  mart.feature_usage_summary grain: year × month × feature_name         │
│  mart.customer_health_score grain: account_id (latest snapshot)        │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Table Schemas — Detailed

### Dimension Tables

#### `warehouse.dim_accounts`
| Column | Type | Constraint | Description |
|---|---|---|---|
| account_id | String | PK | Unique account identifier |
| account_name | String | NOT NULL | Company name |
| industry | String | | Industry vertical |
| country | String | | Billing country |
| signup_date | Date | NOT NULL | First signup date |
| referral_source | String | | How they found us |
| plan_tier | String | FK → dim_plans | Current subscription tier |
| seats | Int32 | | Number of licensed seats |
| is_trial | UInt8 | | Boolean: currently on trial |
| churn_flag | UInt8 | | Boolean: account has churned |
| _source_system | String | | `crm_system` |
| _updated_at | UInt64 | | Epoch ms, latest version wins |

#### `warehouse.dim_plans`
| Column | Type | Constraint | Description |
|---|---|---|---|
| plan_tier | String | PK | Free / Basic / Pro / Enterprise |
| price_per_seat_usd | Float64 | | Monthly cost per seat |
| billing_type | String | | monthly / annual / mixed |
| description | String | | Human-readable description |
| _updated_at | UInt64 | | Version column |

#### `warehouse.dim_date`
| Column | Type | Description |
|---|---|---|
| date_key | Date | PK — every date 2024-01-01 to 2026-12-31 |
| year | UInt16 | |
| quarter | UInt8 | 1–4 |
| month | UInt8 | 1–12 |
| month_name | String | January … December |
| week_of_year | UInt8 | |
| day_of_week | UInt8 | 1=Monday, 7=Sunday |
| is_weekend | UInt8 | Boolean |
| is_month_start | UInt8 | Boolean |
| is_month_end | UInt8 | Boolean |

---

### Fact Tables

#### `warehouse.fact_subscriptions`
| Column | Type | Constraint | Description |
|---|---|---|---|
| subscription_id | String | PK | |
| account_id | String | FK → dim_accounts | |
| start_date | Date | FK → dim_date | |
| end_date | Date | | |
| plan_tier | String | FK → dim_plans | |
| seats | Int32 | | |
| price | Float64 | | Per-seat price |
| discount_value | Float64 | | Discount applied |
| mrr_amount | Float64 | ≥ 0 | Monthly Recurring Revenue |
| arr_amount | Float64 | ≥ 0 | mrr × 12 |
| is_trial | UInt8 | | |
| upgrade_flag | UInt8 | | Upgraded from previous sub |
| downgrade_flag | UInt8 | | Downgraded from previous sub |
| churn_flag | UInt8 | | Churned at end of this period |
| reactivation_flag | UInt8 | | **Derived**: gap > 60d then re-subscribed |
| billing_frequency | String | | monthly / annual |
| auto_renew_flag | UInt8 | | |
| subscription_sequence | Int32 | | **Derived**: rank within account (1=first) |
| days_active | Int32 | ≥ 0 | **Derived**: end_date − start_date |
| _source_system | String | | `transactional_system` |
| _updated_at | UInt64 | | |

#### `warehouse.fact_churn_events`
| Column | Type | Description |
|---|---|---|
| churn_event_id | String | PK |
| account_id | String | FK → dim_accounts |
| churn_date | Date | FK → dim_date |
| reason_code | String | price_too_high / missing_features / bugs / competitor / no_longer_needed |
| refund_amount_usd | Float64 | ≥ 0 |
| preceding_upgrade_flag | UInt8 | Was there an upgrade before churn? |
| preceding_downgrade_flag | UInt8 | Was there a downgrade before churn? |
| is_reactivation | UInt8 | Did they come back after this churn? |
| feedback_text | Nullable(String) | Raw feedback |
| _source_system | String | `transactional_system` |
| _updated_at | UInt64 | |

#### `warehouse.fact_feature_usage`
| Column | Type | Description |
|---|---|---|
| usage_id | String | PK |
| subscription_id | String | FK → fact_subscriptions |
| usage_date | Date | FK → dim_date |
| feature_name | String | dashboard / api / export / automation / integration |
| usage_count | Int32 | ≥ 0 |
| usage_duration_secs | Int32 | ≥ 0 |
| error_count | Int32 | ≥ 0 |
| is_beta_feature | UInt8 | |
| _source_system | String | `application_logs` |
| _updated_at | UInt64 | |

#### `warehouse.fact_support_tickets`
| Column | Type | Description |
|---|---|---|
| ticket_id | String | PK |
| account_id | String | FK → dim_accounts |
| submitted_at | DateTime | FK → dim_date |
| closed_at | Nullable(DateTime) | |
| resolution_time_hours | Float64 | ≥ 0 |
| priority | String | low / medium / high |
| first_response_time_minutes | Int32 | |
| satisfaction_score | Nullable(Float64) | 1.0–5.0 |
| escalation_flag | UInt8 | |
| _source_system | String | `support_system` |
| _updated_at | UInt64 | |

---

## Fact vs Dimension: Design Rationale

| | **Dimensions** | **Facts** |
|---|---|---|
| **Nature** | Descriptive, slowly changing | Transactional, append-heavy |
| **Cardinality** | Low (thousands) | High (millions) |
| **Mutability** | Updated infrequently | Mostly immutable, new rows added daily |
| **Engine** | ReplacingMergeTree | ReplacingMergeTree (partitioned by month) |
| **Examples** | dim_accounts, dim_plans, dim_date | fact_subscriptions, fact_churn_events |
| **FK role** | Provides attributes for grouping | Carries metrics (MRR, ARR, usage count) |

### Why ReplacingMergeTree for everything?
ClickHouse has no SQL `MERGE/UPSERT`. `ReplacingMergeTree(_updated_at)` is the idiomatic pattern:
- Insert new rows with same PK but higher `_updated_at` → engine keeps latest on merge
- Read with `SELECT FINAL` for immediate deduplication
- Safe to re-run the same batch (idempotent by design)
