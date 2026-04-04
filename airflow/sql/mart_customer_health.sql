-- =============================================================================
-- mart_customer_health.sql
-- Customer health score — full snapshot rebuilt per account (latest state)
-- =============================================================================
-- Health score formula (0–100):
--   base     = 50
--   +15  if active subscription (not churned)
--   +10  if MRR > 0 (paying customer)
--   +10  if avg satisfaction >= 4.0
--   +10  if usage in last 30 days > 0
--   -15  if churn_flag = true
--   -10  if open high-priority tickets > 0
--   -10  if days_since_last_activity > 90
--
-- Rebuilt incrementally: DELETE all accounts that had new activity in the
-- batch period, then re-insert their refreshed scores.
-- =============================================================================

-- Step 1: Delete accounts that had activity in the batch window
ALTER TABLE mart.customer_health_score
DELETE WHERE account_id IN (
    -- accounts with new subscriptions
    SELECT DISTINCT account_id
    FROM production.fact_subscriptions FINAL
    WHERE toDate(start_date) > toDate('{from_date}')
      AND toDate(start_date) <= toDate('{to_date}')

    UNION ALL

    -- accounts with new churn events
    SELECT DISTINCT account_id
    FROM production.fact_churn_events FINAL
    WHERE toDate(churn_date) > toDate('{from_date}')
      AND toDate(churn_date) <= toDate('{to_date}')

    UNION ALL

    -- accounts with new support tickets
    SELECT DISTINCT account_id
    FROM production.fact_support_tickets FINAL
    WHERE toDate(submitted_at) > toDate('{from_date}')
      AND toDate(submitted_at) <= toDate('{to_date}')
);

-- Step 2: Re-insert refreshed health scores for affected accounts
INSERT INTO mart.customer_health_score
(account_id, account_name, plan_tier, country, industry, health_score,
 mrr_last_month, avg_satisfaction, total_usage_30d, open_high_tickets,
 churn_risk, days_since_last_activity, _refreshed_at)

WITH

-- Latest subscription per account
latest_sub AS (
    SELECT
        account_id,
        plan_tier,
        mrr_amount,
        churn_flag,
        start_date,
        end_date,
        ROW_NUMBER() OVER (PARTITION BY account_id ORDER BY start_date DESC) AS rn
    FROM production.fact_subscriptions FINAL
),

-- Last 30 days usage per account (via subscription linkage)
usage_30d AS (
    SELECT
        s.account_id,
        sum(u.usage_count) AS usage_cnt
    FROM production.fact_feature_usage FINAL u
    JOIN production.fact_subscriptions FINAL s USING (subscription_id)
    WHERE u.usage_date >= (today() - 30)
    GROUP BY s.account_id
),

-- Average satisfaction score per account
avg_sat AS (
    SELECT
        account_id,
        avg(satisfaction_score) AS avg_score
    FROM production.fact_support_tickets FINAL
    WHERE satisfaction_score IS NOT NULL
    GROUP BY account_id
),

-- Open high-priority tickets
high_tickets AS (
    SELECT
        account_id,
        countIf(priority = 'high' AND closed_at IS NULL) AS cnt
    FROM production.fact_support_tickets FINAL
    GROUP BY account_id
),

-- Days since last activity (latest event date across all fact tables)
last_activity AS (
    SELECT account_id, max(start_date) AS last_dt
    FROM production.fact_subscriptions FINAL
    GROUP BY account_id
),

-- Base account info (from affected accounts only)
affected_accounts AS (
    SELECT DISTINCT account_id FROM production.fact_subscriptions FINAL
    WHERE toDate(start_date) > toDate('{from_date}')
      AND toDate(start_date) <= toDate('{to_date}')
    UNION ALL
    SELECT DISTINCT account_id FROM production.fact_churn_events FINAL
    WHERE toDate(churn_date) > toDate('{from_date}')
      AND toDate(churn_date) <= toDate('{to_date}')
    UNION ALL
    SELECT DISTINCT account_id FROM production.fact_support_tickets FINAL
    WHERE toDate(submitted_at) > toDate('{from_date}')
      AND toDate(submitted_at) <= toDate('{to_date}')
)

SELECT
    a.account_id,
    a.account_name,
    coalesce(ls.plan_tier, a.plan_tier)                     AS plan_tier,
    a.country,
    a.industry,

    -- Health score calculation
    greatest(0, least(100,
        50
        + if(ls.churn_flag = 0, 15, 0)
        + if(ls.mrr_amount > 0, 10, 0)
        + if(sat.avg_score >= 4.0, 10, 0)
        + if(u30.usage_cnt > 0, 10, 0)
        - if(ls.churn_flag = 1, 15, 0)
        - if(ht.cnt > 0, 10, 0)
        - if(dateDiff('day', la.last_dt, today()) > 90, 10, 0)
    ))                                                      AS health_score,

    round(coalesce(ls.mrr_amount, 0), 2)                    AS mrr_last_month,
    round(sat.avg_score, 2)                                 AS avg_satisfaction,
    coalesce(u30.usage_cnt, 0)                              AS total_usage_30d,
    coalesce(ht.cnt, 0)                                     AS open_high_tickets,

    -- Churn risk label
    multiIf(
        ls.churn_flag = 1, 'high',
        coalesce(u30.usage_cnt, 0) = 0, 'high',
        dateDiff('day', la.last_dt, today()) > 90, 'medium',
        coalesce(sat.avg_score, 5) < 3.0, 'medium',
        'low'
    )                                                       AS churn_risk,

    dateDiff('day', la.last_dt, today())                    AS days_since_last_activity,
    now()                                                   AS _refreshed_at

FROM production.dim_accounts FINAL a
JOIN affected_accounts aa ON a.account_id = aa.account_id
LEFT JOIN (SELECT * FROM latest_sub WHERE rn = 1) ls ON a.account_id = ls.account_id
LEFT JOIN usage_30d u30 ON a.account_id = u30.account_id
LEFT JOIN avg_sat sat ON a.account_id = sat.account_id
LEFT JOIN high_tickets ht ON a.account_id = ht.account_id
LEFT JOIN last_activity la ON a.account_id = la.account_id;
