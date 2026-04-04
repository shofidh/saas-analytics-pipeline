{{
  config(
    materialized  = 'incremental',
    schema        = 'mart',
    engine        = 'ReplacingMergeTree()',
    order_by      = '(year, month, plan_tier)',
    unique_key    = ['year', 'month', 'plan_tier'],
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns'
  )
}}

/*
  mart_mrr_monthly
  ================
  Grain : year × month × plan_tier
  Logic :
    - active_accounts  : subscriptions spanning this month
    - new_accounts     : first-ever subscription started this month
    - churned_accounts : churn events recorded this month
    - total_mrr / arr  : sum of active subscription revenue
    - avg_mrr          : per account average

  Incremental: on each run, only reprocesses months where
  new subscription data has arrived (based on start_date).
*/

WITH

active_subs AS (
    SELECT
        account_id,
        plan_tier,
        mrr_amount,
        arr_amount,
        seats,
        churn_flag,
        start_date,
        end_date,
        subscription_sequence,
        toYear(start_date)  AS sub_year,
        toMonth(start_date) AS sub_month
    FROM {{ source('production', 'fact_subscriptions') }} FINAL
    WHERE
        -- Only months we need to refresh (incremental filter)
        {% if is_incremental() %}
          toYYYYMM(start_date) >= (
            SELECT coalesce(max(year * 100 + month), toYYYYMM(toDate('2024-01-01')))
            FROM {{ this }}
          )
        {% else %}
          1 = 1
        {% endif %}
),

-- Unique active months in scope
months_in_scope AS (
    SELECT DISTINCT
        toYear(m)  AS year,
        toMonth(m) AS month
    FROM (
        -- Generate month-start dates for each active subscription month
        SELECT arrayJoin(
            arrayMap(
                d -> toDate(toStartOfMonth(toDate('2024-01-01') + toIntervalDay(d * 30))),
                range(toUInt64(dateDiff('month', toDate('2024-01-01'), today()) + 1))
            )
        ) AS m
    )
    WHERE 1 = 1
    {% if is_incremental() %}
      AND toYYYYMM(m) >= (
        SELECT coalesce(max(year * 100 + month), toYYYYMM(toDate('2024-01-01')))
        FROM {{ this }}
      )
    {% endif %}
),

-- New accounts: subscription_sequence = 1 AND started this month
new_accts AS (
    SELECT
        toYear(start_date)  AS year,
        toMonth(start_date) AS month,
        account_id
    FROM {{ source('production', 'fact_subscriptions') }} FINAL
    WHERE subscription_sequence = 1
    {% if is_incremental() %}
      AND toYYYYMM(start_date) >= (
        SELECT coalesce(max(year * 100 + month), toYYYYMM(toDate('2024-01-01')))
        FROM {{ this }}
      )
    {% endif %}
),

-- Churned accounts per month
churned_accts AS (
    SELECT
        toYear(churn_date)  AS year,
        toMonth(churn_date) AS month,
        account_id
    FROM {{ source('production', 'fact_churn_events') }} FINAL
    {% if is_incremental() %}
    WHERE toYYYYMM(churn_date) >= (
      SELECT coalesce(max(year * 100 + month), toYYYYMM(toDate('2024-01-01')))
      FROM {{ this }}
    )
    {% endif %}
),

-- Active subscriptions per month: subscription spans that month
active_per_month AS (
    SELECT
        m.year,
        m.month,
        s.account_id,
        s.plan_tier,
        s.mrr_amount,
        s.arr_amount,
        s.seats
    FROM months_in_scope m
    JOIN active_subs s
      ON  s.sub_year  <= m.year
      AND s.sub_month <= m.month
      AND (
          toYear(s.end_date)  > m.year
          OR (toYear(s.end_date) = m.year AND toMonth(s.end_date) >= m.month)
      )
)

SELECT
    a.year,
    a.month,
    a.plan_tier,
    countDistinct(a.account_id)                                                         AS active_accounts,
    countDistinctIf(a.account_id, (a.year, a.month, a.account_id) IN (
        SELECT year, month, account_id FROM new_accts))                                 AS new_accounts,
    countDistinctIf(a.account_id, (a.year, a.month, a.account_id) IN (
        SELECT year, month, account_id FROM churned_accts))                             AS churned_accounts,
    round(sum(a.mrr_amount), 2)                                                         AS total_mrr,
    round(sum(a.arr_amount), 2)                                                         AS total_arr,
    round(avg(a.mrr_amount), 2)                                                         AS avg_mrr_per_account,
    toUInt32(sum(a.seats))                                                              AS total_seats,
    now()                                                                               AS _refreshed_at
FROM active_per_month a
GROUP BY a.year, a.month, a.plan_tier
ORDER BY a.year, a.month, total_mrr DESC
