

/*
  mart_feature_usage_summary
  ==========================
  Grain : year × month × feature_name
  Metrics:
    - total_usage_count     : total feature invocations
    - total_duration_secs   : total time spent using feature
    - total_error_count     : total errors encountered
    - unique_subscriptions  : distinct subscribers using the feature
    - avg_usage_per_sub     : total_usage / unique_subscriptions
    - beta_usage_count      : invocations of beta features
*/

SELECT
    toYear(usage_date)                                              AS year,
    toMonth(usage_date)                                             AS month,
    feature_name,
    toUInt64(sum(usage_count))                                      AS total_usage_count,
    toUInt64(sum(usage_duration_secs))                              AS total_duration_secs,
    toUInt32(sum(error_count))                                      AS total_error_count,
    toUInt32(countDistinct(subscription_id))                        AS unique_subscriptions,
    round(
        sum(usage_count) / nullIf(countDistinct(subscription_id), 0),
        2
    )                                                               AS avg_usage_per_sub,
    toUInt32(countIf(is_beta_feature = 1))                          AS beta_usage_count,
    now()                                                           AS _refreshed_at
FROM `default`.`fact_feature_usage` FINAL
WHERE 1 = 1

GROUP BY year, month, feature_name
ORDER BY year, month, total_usage_count DESC