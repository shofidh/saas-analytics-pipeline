-- =============================================================================
-- ddl_warehouse.sql
-- ClickHouse DDL — SaaS Analytics Platform
-- Layers: staging → warehouse (dims + facts) → mart
-- =============================================================================

-- ─────────────────────────────────────────────────────────────────────────────
-- 0. DATABASES
-- ─────────────────────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS staging;
CREATE DATABASE IF NOT EXISTS warehouse;
CREATE DATABASE IF NOT EXISTS mart;


-- =============================================================================
-- 1. STAGING LAYER  (raw ingest buffer; no FK enforcement)
-- Engine: MergeTree — high-throughput inserts
-- =============================================================================

CREATE TABLE IF NOT EXISTS staging.stg_accounts
(
    account_id          String,
    account_name        String,
    industry            String,
    country             String,
    signup_date         Date,
    referral_source     String,
    plan_tier           String,
    seats               Int32,
    is_trial            UInt8,
    churn_flag          UInt8,
    _ingestion_time     String,
    _source_system      String,
    _batch_id           String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(signup_date)
ORDER BY (account_id, signup_date);

CREATE TABLE IF NOT EXISTS staging.stg_subscriptions
(
    subscription_id     String,
    account_id          String,
    start_date          Date,
    end_date            Date,
    plan_tier           String,
    seats               Int32,
    price               Float64,
    discount_value      Float64,
    mrr_amount          Float64,
    arr_amount          Float64,
    is_trial            UInt8,
    upgrade_flag        UInt8,
    downgrade_flag      UInt8,
    churn_flag          UInt8,
    reactivation_flag   UInt8,
    billing_frequency   String,
    auto_renew_flag     UInt8,
    subscription_sequence Int32,
    days_active         Int32,
    _ingestion_time     String,
    _source_system      String,
    _batch_id           String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(start_date)
ORDER BY (account_id, start_date, subscription_id);

CREATE TABLE IF NOT EXISTS staging.stg_churn_events
(
    churn_event_id              String,
    account_id                  String,
    churn_date                  Date,
    reason_code                 String,
    refund_amount_usd           Float64,
    preceding_upgrade_flag      UInt8,
    preceding_downgrade_flag    UInt8,
    is_reactivation             UInt8,
    feedback_text               Nullable(String),
    _ingestion_time             String,
    _source_system              String,
    _batch_id                   String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(churn_date)
ORDER BY (account_id, churn_date, churn_event_id);

CREATE TABLE IF NOT EXISTS staging.stg_feature_usage
(
    usage_id                String,
    subscription_id         String,
    usage_date              Date,
    feature_name            String,
    usage_count             Int32,
    usage_duration_secs     Int32,
    error_count             Int32,
    is_beta_feature         UInt8,
    _ingestion_time         String,
    _source_system          String,
    _batch_id               String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(usage_date)
ORDER BY (subscription_id, usage_date, usage_id);

CREATE TABLE IF NOT EXISTS staging.stg_support_tickets
(
    ticket_id                       String,
    account_id                      String,
    submitted_at                    DateTime,
    closed_at                       Nullable(DateTime),
    resolution_time_hours           Float64,
    priority                        String,
    first_response_time_minutes     Int32,
    satisfaction_score              Nullable(Float64),
    escalation_flag                 UInt8,
    _ingestion_time                 String,
    _source_system                  String,
    _batch_id                       String
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(submitted_at)
ORDER BY (account_id, submitted_at, ticket_id);


-- =============================================================================
-- 2. WAREHOUSE — DIMENSION TABLES
-- Engine: ReplacingMergeTree(_updated_at) — idempotent upserts
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.dim_accounts
(
    account_id          String,
    account_name        String,
    industry            String,
    country             String,
    signup_date         Date,
    referral_source     String,
    plan_tier           String,
    seats               Int32,
    is_trial            UInt8,
    churn_flag          UInt8,
    _source_system      String,
    _updated_at         UInt64
)
ENGINE = ReplacingMergeTree(_updated_at)
PARTITION BY toYear(signup_date)
ORDER BY account_id;

CREATE TABLE IF NOT EXISTS warehouse.dim_plans
(
    plan_tier           String,
    price_per_seat_usd  Float64,
    billing_type        String,
    description         String,
    _updated_at         UInt64
)
ENGINE = ReplacingMergeTree(_updated_at)
ORDER BY plan_tier;

-- Seed plan data (idempotent — ReplacingMergeTree deduplicates on merge)
INSERT INTO warehouse.dim_plans VALUES
    ('Free',       0.00,  'none',    'Free tier, no billing',               1000),
    ('Basic',      10.00, 'monthly', 'Basic plan, per seat per month',      1000),
    ('Pro',        25.00, 'monthly', 'Pro plan, per seat per month',        1000),
    ('Enterprise', 60.00, 'mixed',   'Enterprise, negotiated pricing',      1000);

-- dim_date: date spine 2024-01-01 → 2026-12-31
CREATE TABLE IF NOT EXISTS warehouse.dim_date
(
    date_key        Date,
    year            UInt16,
    quarter         UInt8,
    month           UInt8,
    month_name      String,
    week_of_year    UInt8,
    day_of_month    UInt8,
    day_of_week     UInt8,
    day_name        String,
    is_weekend      UInt8,
    is_month_start  UInt8,
    is_month_end    UInt8
)
ENGINE = MergeTree()
ORDER BY date_key;

-- Populate date spine using numbers() — compute date_key inline, no forward ref
-- Note: %B and %A are not supported in ClickHouse 26.x; using CASE instead
INSERT INTO warehouse.dim_date
SELECT
    d                                                       AS date_key,
    toYear(d)                                               AS year,
    toQuarter(d)                                            AS quarter,
    toMonth(d)                                              AS month,
    CASE toMonth(d)
        WHEN 1  THEN 'January'   WHEN 2  THEN 'February'
        WHEN 3  THEN 'March'     WHEN 4  THEN 'April'
        WHEN 5  THEN 'May'       WHEN 6  THEN 'June'
        WHEN 7  THEN 'July'      WHEN 8  THEN 'August'
        WHEN 9  THEN 'September' WHEN 10 THEN 'October'
        WHEN 11 THEN 'November'  ELSE    'December'
    END                                                     AS month_name,
    toWeek(d)                                               AS week_of_year,
    toDayOfMonth(d)                                         AS day_of_month,
    toDayOfWeek(d)                                          AS day_of_week,
    CASE toDayOfWeek(d)
        WHEN 1 THEN 'Monday'    WHEN 2 THEN 'Tuesday'
        WHEN 3 THEN 'Wednesday' WHEN 4 THEN 'Thursday'
        WHEN 5 THEN 'Friday'    WHEN 6 THEN 'Saturday'
        ELSE 'Sunday'
    END                                                     AS day_name,
    if(toDayOfWeek(d) IN (6, 7), 1, 0)                     AS is_weekend,
    if(toDayOfMonth(d) = 1, 1, 0)                          AS is_month_start,
    if(d = toLastDayOfMonth(d), 1, 0)                      AS is_month_end
FROM (
    SELECT toDate(addDays(toDate('2024-01-01'), number)) AS d
    FROM numbers(1096)   -- 365 (2024) + 365 (2025) + 366 (2026) = 1096
)
WHERE d <= toDate('2026-12-31');


-- =============================================================================
-- 3. WAREHOUSE — FACT TABLES
-- Engine: ReplacingMergeTree(_updated_at) for late-arriving data handling
-- =============================================================================

CREATE TABLE IF NOT EXISTS warehouse.fact_subscriptions
(
    subscription_id         String,
    account_id              String,
    start_date              Date,
    end_date                Date,
    plan_tier               String,
    seats                   Int32,
    price                   Float64,
    discount_value          Float64,
    mrr_amount              Float64,
    arr_amount              Float64,
    is_trial                UInt8,
    upgrade_flag            UInt8,
    downgrade_flag          UInt8,
    churn_flag              UInt8,
    reactivation_flag       UInt8,
    billing_frequency       String,
    auto_renew_flag         UInt8,
    subscription_sequence   Int32,
    days_active             Int32,
    _source_system          String,
    _updated_at             UInt64
)
ENGINE = ReplacingMergeTree(_updated_at)
PARTITION BY toYYYYMM(start_date)
ORDER BY (account_id, start_date, subscription_id);

CREATE TABLE IF NOT EXISTS warehouse.fact_churn_events
(
    churn_event_id              String,
    account_id                  String,
    churn_date                  Date,
    reason_code                 String,
    refund_amount_usd           Float64,
    preceding_upgrade_flag      UInt8,
    preceding_downgrade_flag    UInt8,
    is_reactivation             UInt8,
    feedback_text               Nullable(String),
    _source_system              String,
    _updated_at                 UInt64
)
ENGINE = ReplacingMergeTree(_updated_at)
PARTITION BY toYYYYMM(churn_date)
ORDER BY (account_id, churn_date, churn_event_id);

CREATE TABLE IF NOT EXISTS warehouse.fact_feature_usage
(
    usage_id                String,
    subscription_id         String,
    usage_date              Date,
    feature_name            String,
    usage_count             Int32,
    usage_duration_secs     Int32,
    error_count             Int32,
    is_beta_feature         UInt8,
    _source_system          String,
    _updated_at             UInt64
)
ENGINE = ReplacingMergeTree(_updated_at)
PARTITION BY toYYYYMM(usage_date)
ORDER BY (subscription_id, usage_date, usage_id);

CREATE TABLE IF NOT EXISTS warehouse.fact_support_tickets
(
    ticket_id                       String,
    account_id                      String,
    submitted_at                    DateTime,
    closed_at                       Nullable(DateTime),
    resolution_time_hours           Float64,
    priority                        String,
    first_response_time_minutes     Int32,
    satisfaction_score              Nullable(Float64),
    escalation_flag                 UInt8,
    _source_system                  String,
    _updated_at                     UInt64
)
ENGINE = ReplacingMergeTree(_updated_at)
PARTITION BY toYYYYMM(submitted_at)
ORDER BY (account_id, submitted_at, ticket_id);


-- =============================================================================
-- 4. DATA MART LAYER  (built by dbt; DDL here for reference / cold-start)
-- =============================================================================

CREATE TABLE IF NOT EXISTS mart.mrr_monthly
(
    year                UInt16,
    month               UInt8,
    plan_tier           String,
    active_accounts     UInt32,
    new_accounts        UInt32,
    churned_accounts    UInt32,
    total_mrr           Float64,
    total_arr           Float64,
    avg_mrr_per_account Float64,
    total_seats         UInt32,
    _refreshed_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_refreshed_at)
ORDER BY (year, month, plan_tier);

CREATE TABLE IF NOT EXISTS mart.churn_summary
(
    year                UInt16,
    month               UInt8,
    reason_code         String,
    churn_count         UInt32,
    total_refund_usd    Float64,
    reactivation_count  UInt32,
    churn_rate_pct      Float64,
    _refreshed_at       DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_refreshed_at)
ORDER BY (year, month, reason_code);

CREATE TABLE IF NOT EXISTS mart.feature_usage_summary
(
    year                    UInt16,
    month                   UInt8,
    feature_name            String,
    total_usage_count       UInt64,
    total_duration_secs     UInt64,
    total_error_count       UInt32,
    unique_subscriptions    UInt32,
    avg_usage_per_sub       Float64,
    beta_usage_count        UInt32,
    _refreshed_at           DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_refreshed_at)
ORDER BY (year, month, feature_name);

CREATE TABLE IF NOT EXISTS mart.customer_health_score
(
    account_id              String,
    account_name            String,
    plan_tier               String,
    country                 String,
    industry                String,
    health_score            Float64,
    mrr_last_month          Float64,
    avg_satisfaction        Nullable(Float64),
    total_usage_30d         UInt32,
    open_high_tickets       UInt32,
    churn_risk              String,
    days_since_last_activity Int32,
    _refreshed_at           DateTime DEFAULT now()
)
ENGINE = ReplacingMergeTree(_refreshed_at)
ORDER BY account_id;
