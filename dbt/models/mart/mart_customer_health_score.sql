{{
  config(
    materialized  = 'incremental',
    schema        = 'mart',
    alias         = 'customer_health_score',
    engine        = 'ReplacingMergeTree()',
    order_by      = '(account_id)',
    unique_key    = 'account_id',
    incremental_strategy = 'delete+insert',
    on_schema_change = 'sync_all_columns'
  )
}}

/*
  mart_customer_health_score
  ==========================
  Grain : account_id (latest snapshot per account)

  Health Score Formula (0–100):
    base  =  50
    +15   active subscription (churn_flag = 0)
    +10   paying customer (mrr > 0)
    +10   avg satisfaction ≥ 4.0
    +10   used at least 1 feature in last 30 days
    -15   churned (churn_flag = 1)
    -10   has open high-priority tickets
    -10   no activity in last 90 days

  Churn risk label:
    'high'   → churn_flag = 1 OR no usage in 30d
    'medium' → no activity 90d OR satisfaction < 3
    'low'    → everything else

  Incremental: only recalculates accounts that had new events
  (subscription start, churn, or ticket) in the latest batch window.
*/

-- Affected accounts: had new activity in the batch period (or all accounts on first/empty run)
WITH affected AS (
    SELECT DISTINCT account_id
    FROM {{ source('production', 'fact_subscriptions') }} FINAL
    WHERE 1 = 1
    {% if is_incremental() %}
      AND toDate(start_date) >= (
        SELECT if(count(*) = 0, toDate('2024-01-01'), today() - 1)
        FROM {{ this }}
      )
    {% endif %}

    UNION DISTINCT

    SELECT DISTINCT account_id
    FROM {{ source('production', 'fact_churn_events') }} FINAL
    WHERE 1 = 1
    {% if is_incremental() %}
      AND toDate(churn_date) >= (
        SELECT if(count(*) = 0, toDate('2024-01-01'), today() - 1)
        FROM {{ this }}
      )
    {% endif %}

    UNION DISTINCT

    SELECT DISTINCT account_id
    FROM {{ source('production', 'fact_support_tickets') }} FINAL
    WHERE 1 = 1
    {% if is_incremental() %}
      AND toDate(submitted_at) >= (
        SELECT if(count(*) = 0, toDate('2024-01-01'), today() - 1)
        FROM {{ this }}
      )
    {% endif %}
),

-- Latest subscription per account (most recent start_date)
latest_sub AS (
    SELECT
        account_id,
        plan_tier,
        mrr_amount,
        churn_flag,
        start_date,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY start_date DESC) AS rn
    FROM {{ source('production', 'fact_subscriptions') }} FINAL
),
latest_sub_1 AS (
    SELECT * FROM latest_sub WHERE rn = 1
),

-- Feature usage in last 30 days (linked via subscription → account)
usage_30d AS (
    SELECT
        s.account_id,
        sum(u.usage_count) AS usage_cnt
    FROM (
        SELECT subscription_id, usage_count, usage_date
        FROM {{ source('production', 'fact_feature_usage') }} FINAL
    ) AS u
    JOIN (
        SELECT account_id, subscription_id
        FROM {{ source('production', 'fact_subscriptions') }} FINAL
    ) AS s ON u.subscription_id = s.subscription_id
    WHERE u.usage_date >= (today() - 30)
    GROUP BY s.account_id
),

-- Average satisfaction score per account
avg_sat AS (
    SELECT
        account_id,
        avg(satisfaction_score) AS avg_score
    FROM {{ source('production', 'fact_support_tickets') }} FINAL
    WHERE satisfaction_score IS NOT NULL
    GROUP BY account_id
),

-- Open high-priority tickets per account
high_tix AS (
    SELECT
        account_id,
        countIf(priority = 'high' AND closed_at IS NULL) AS open_high_cnt
    FROM {{ source('production', 'fact_support_tickets') }} FINAL
    GROUP BY account_id
),

-- Days since last subscription activity
last_activity AS (
    SELECT
        account_id,
        max(start_date) AS last_sub_date
    FROM {{ source('production', 'fact_subscriptions') }} FINAL
    GROUP BY account_id
)

SELECT
    a.account_id                                                                 AS account_id,
    a.account_name                                                               AS account_name,
    coalesce(ls.plan_tier, a.plan_tier)                                     AS plan_tier,
    a.country,
    a.industry,

    -- ── Health score ──────────────────────────────────────────────────────
    greatest(0, least(100,
          50
        + if(ls.churn_flag = 0,    15, 0)
        + if(ls.mrr_amount > 0,    10, 0)
        + if(sat.avg_score >= 4.0, 10, 0)
        + if(u30.usage_cnt > 0,    10, 0)
        - if(ls.churn_flag = 1,    15, 0)
        - if(ht.open_high_cnt > 0, 10, 0)
        - if(dateDiff('day', la.last_sub_date, today()) > 90, 10, 0)
    ))                                                                      AS health_score,

    round(coalesce(ls.mrr_amount, 0), 2)                                    AS mrr_last_month,
    round(sat.avg_score, 2)                                                 AS avg_satisfaction,
    toUInt32(coalesce(u30.usage_cnt, 0))                                    AS total_usage_30d,
    toUInt32(coalesce(ht.open_high_cnt, 0))                                 AS open_high_tickets,

    -- ── Churn risk label ─────────────────────────────────────────────────
    multiIf(
        ls.churn_flag = 1,                                              'high',
        coalesce(u30.usage_cnt, 0) = 0,                                'high',
        dateDiff('day', la.last_sub_date, today()) > 90,               'medium',
        coalesce(sat.avg_score, 5.0) < 3.0,                           'medium',
        'low'
    )                                                                       AS churn_risk,

    dateDiff('day', la.last_sub_date, today())                              AS days_since_last_activity,
    now()                                                                   AS _refreshed_at

FROM (
    SELECT account_id, account_name, plan_tier, country, industry
    FROM {{ source('production', 'dim_accounts') }} FINAL
) AS a
JOIN affected AS af ON a.account_id = af.account_id
LEFT JOIN latest_sub_1 ls  ON a.account_id = ls.account_id
LEFT JOIN usage_30d u30    ON a.account_id = u30.account_id
LEFT JOIN avg_sat sat      ON a.account_id = sat.account_id
LEFT JOIN high_tix ht      ON a.account_id = ht.account_id
LEFT JOIN last_activity la ON a.account_id = la.account_id
