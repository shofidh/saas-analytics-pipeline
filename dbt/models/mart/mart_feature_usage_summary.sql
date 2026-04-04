{{
  config(
    materialized  = 'incremental',
    schema        = 'mart',
    engine        = 'ReplacingMergeTree()',
    order_by      = '(year, month, feature_name)',
    unique_key    = ['year', 'month', 'feature_name'],
    incremental_strategy = 'delete+insert',
    on_schema_change = 'append_new_columns'
  )
}}

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
FROM {{ source('production', 'fact_feature_usage') }} FINAL
WHERE 1 = 1
{% if is_incremental() %}
  AND toYYYYMM(usage_date) >= (
    SELECT coalesce(max(year * 100 + month), toYYYYMM(toDate('2024-01-01')))
    FROM {{ this }}
  )
{% endif %}
GROUP BY year, month, feature_name
ORDER BY year, month, total_usage_count DESC
