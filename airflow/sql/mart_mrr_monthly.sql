-- =============================================================================
-- mart_mrr_monthly.sql
-- Incremental MRR aggregation — only recomputes the latest month partition
-- =============================================================================
-- Grain: year × month × plan_tier
-- Incremental: DELETE + INSERT only for the target month (no full recompute)
-- =============================================================================

-- Step 1: Delete the target month partition (to allow re-run safely)
ALTER TABLE mart.mrr_monthly
DELETE WHERE year = {year:UInt16} AND month = {month:UInt8};

-- Step 2: Insert fresh aggregation for the target month only
INSERT INTO mart.mrr_monthly
(year, month, plan_tier, active_accounts, new_accounts, churned_accounts,
 total_mrr, total_arr, avg_mrr_per_account, total_seats, _refreshed_at)

WITH

-- Active subscriptions in the target month
active_subs AS (
    SELECT
        account_id,
        plan_tier,
        mrr_amount,
        arr_amount,
        seats,
        churn_flag,
        subscription_sequence,
        toYear(start_date)  AS sub_year,
        toMonth(start_date) AS sub_month
    FROM warehouse.fact_subscriptions FINAL
    WHERE
        toYear(start_date)  <= {year:UInt16}
        AND toMonth(start_date) <= {month:UInt8}
        AND (
            -- subscription still active (end_date in or after target month)
            toYear(end_date)  > {year:UInt16}
            OR (toYear(end_date) = {year:UInt16} AND toMonth(end_date) >= {month:UInt8})
        )
),

-- New accounts that started in this month
new_accts AS (
    SELECT account_id
    FROM warehouse.fact_subscriptions FINAL
    WHERE toYear(start_date) = {year:UInt16}
      AND toMonth(start_date) = {month:UInt8}
      AND subscription_sequence = 1      -- very first subscription
    GROUP BY account_id
),

-- Churned accounts in this month
churned_accts AS (
    SELECT account_id
    FROM warehouse.fact_churn_events FINAL
    WHERE toYear(churn_date) = {year:UInt16}
      AND toMonth(churn_date) = {month:UInt8}
    GROUP BY account_id
)

SELECT
    {year:UInt16}                                       AS year,
    {month:UInt8}                                       AS month,
    s.plan_tier,
    countDistinct(s.account_id)                         AS active_accounts,
    countDistinctIf(s.account_id, s.account_id IN (SELECT account_id FROM new_accts))
                                                        AS new_accounts,
    countDistinctIf(s.account_id, s.account_id IN (SELECT account_id FROM churned_accts))
                                                        AS churned_accounts,
    round(sum(s.mrr_amount), 2)                         AS total_mrr,
    round(sum(s.arr_amount), 2)                         AS total_arr,
    round(avg(s.mrr_amount), 2)                         AS avg_mrr_per_account,
    sum(s.seats)                                        AS total_seats,
    now()                                               AS _refreshed_at
FROM active_subs s
GROUP BY s.plan_tier
ORDER BY total_mrr DESC;
