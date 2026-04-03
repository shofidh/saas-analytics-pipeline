{{
  config(
    materialized  = 'incremental',
    schema        = 'mart',
    engine        = 'ReplacingMergeTree()',
    order_by      = '(year, month, reason_code)',
    unique_key    = ['year', 'month', 'reason_code'],
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns'
  )
}}

/*
  mart_churn_summary
  ==================
  Grain : year × month × reason_code
  Metrics:
    - churn_count        : number of churn events
    - total_refund_usd   : total refunds issued
    - reactivation_count : how many came back after churning
    - churn_rate_pct     : churned / active_at_start × 100
*/

WITH

churn_events AS (
    SELECT
        toYear(churn_date)  AS year,
        toMonth(churn_date) AS month,
        reason_code,
        count()                         AS churn_count,
        sum(refund_amount_usd)          AS total_refund_usd,
        countIf(is_reactivation = 1)    AS reactivation_count
    FROM {{ source('warehouse', 'fact_churn_events') }} FINAL
    WHERE 1 = 1
    {% if is_incremental() %}
      AND toYYYYMM(churn_date) >= toYYYYMM(now() - toIntervalMonth(1))
    {% endif %}
    GROUP BY year, month, reason_code
),

-- Active account count at the start of each target month (denominator)
active_counts AS (
    SELECT
        toYear(start_date)  AS year,
        toMonth(start_date) AS month,
        countDistinct(account_id) AS active_cnt
    FROM {{ source('warehouse', 'fact_subscriptions') }} FINAL
    WHERE 1 = 1
    {% if is_incremental() %}
      AND toYYYYMM(start_date) >= toYYYYMM(now() - toIntervalMonth(1))
    {% endif %}
    GROUP BY year, month
)

SELECT
    c.year,
    c.month,
    c.reason_code,
    c.churn_count,
    round(c.total_refund_usd, 2)                                AS total_refund_usd,
    c.reactivation_count,
    round(
        if(a.active_cnt > 0, (c.churn_count / a.active_cnt) * 100, 0),
        2
    )                                                           AS churn_rate_pct,
    now()                                                       AS _refreshed_at
FROM churn_events c
LEFT JOIN active_counts a
  ON c.year = a.year AND c.month = a.month
ORDER BY c.year, c.month, c.churn_count DESC
