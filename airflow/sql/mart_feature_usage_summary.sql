-- =============================================================================
-- mart_feature_usage_summary.sql
-- Incremental feature usage aggregation — latest month partition only
-- =============================================================================
-- Grain: year × month × feature_name
-- =============================================================================

-- Step 1: Delete the target month
ALTER TABLE mart.feature_usage_summary
DELETE WHERE year = {year:UInt16} AND month = {month:UInt8};

-- Step 2: Insert fresh aggregation for target month
INSERT INTO mart.feature_usage_summary
(year, month, feature_name, total_usage_count, total_duration_secs,
 total_error_count, unique_subscriptions, avg_usage_per_sub, beta_usage_count, _refreshed_at)

SELECT
    {year:UInt16}                                       AS year,
    {month:UInt8}                                       AS month,
    feature_name,
    sum(usage_count)                                    AS total_usage_count,
    sum(usage_duration_secs)                            AS total_duration_secs,
    sum(error_count)                                    AS total_error_count,
    countDistinct(subscription_id)                      AS unique_subscriptions,
    round(sum(usage_count) / countDistinct(subscription_id), 2)
                                                        AS avg_usage_per_sub,
    countIf(is_beta_feature = 1)                        AS beta_usage_count,
    now()                                               AS _refreshed_at
FROM warehouse.fact_feature_usage FINAL
WHERE toYear(usage_date) = {year:UInt16}
  AND toMonth(usage_date) = {month:UInt8}
GROUP BY feature_name
ORDER BY total_usage_count DESC;
