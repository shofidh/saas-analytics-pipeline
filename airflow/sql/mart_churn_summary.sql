-- =============================================================================
-- mart_churn_summary.sql
-- Incremental churn aggregation — only latest month partition refreshed
-- =============================================================================
-- Grain: year × month × reason_code
-- Includes: churn count, refund totals, reactivation count, churn rate %
-- =============================================================================

-- Step 1: Delete the target month
ALTER TABLE mart.churn_summary
DELETE WHERE year = {year:UInt16} AND month = {month:UInt8};

-- Step 2: Insert fresh data for target month
INSERT INTO mart.churn_summary
(year, month, reason_code, churn_count, total_refund_usd,
 reactivation_count, churn_rate_pct, _refreshed_at)

WITH

-- Active accounts at start of target month (denominator for churn rate)
active_start AS (
    SELECT countDistinct(account_id) AS cnt
    FROM production.fact_subscriptions FINAL
    WHERE
        toYear(start_date) <= {year:UInt16}
        AND toMonth(start_date) <= {month:UInt8}
        AND (
            toYear(end_date) > {year:UInt16}
            OR (toYear(end_date) = {year:UInt16} AND toMonth(end_date) >= {month:UInt8})
        )
),

-- Churn events in target month
churn_events AS (
    SELECT
        reason_code,
        count()                         AS churn_count,
        sum(refund_amount_usd)          AS total_refund_usd,
        countIf(is_reactivation = 1)    AS reactivation_count
    FROM production.fact_churn_events FINAL
    WHERE toYear(churn_date) = {year:UInt16}
      AND toMonth(churn_date) = {month:UInt8}
    GROUP BY reason_code
)

SELECT
    {year:UInt16}                                       AS year,
    {month:UInt8}                                       AS month,
    c.reason_code,
    c.churn_count,
    round(c.total_refund_usd, 2)                        AS total_refund_usd,
    c.reactivation_count,
    round(
        if(a.cnt > 0, (c.churn_count / a.cnt) * 100, 0),
        2
    )                                                   AS churn_rate_pct,
    now()                                               AS _refreshed_at
FROM churn_events c
CROSS JOIN active_start a
ORDER BY c.churn_count DESC;
