CREATE DATABASE staging;

CREATE TABLE staging.accounts
(
    account_id String,
    account_name String,
    industry String,
    country String,
    signup_date Date,
    referral_source String,
    plan_tier String,
    seats UInt16,
    is_trial Bool,
    churn_flag Bool
)
ENGINE = MergeTree
ORDER BY account_id;


CREATE TABLE staging.subscriptions
(
    subscription_id String,
    account_id String,
    start_date Date,
    end_date Nullable(Date),
    plan_tier String,
    seats UInt16,
    mrr_amount Float64,
    arr_amount Float64,
    is_trial Bool,
    upgrade_flag Bool,
    downgrade_flag Bool,
    churn_flag Bool,
    billing_frequency String,
    auto_renew_flag Bool
)
ENGINE = MergeTree
ORDER BY subscription_id;

CREATE TABLE staging.feature_usage
(
    usage_id String,
    subscription_id String,
    usage_date Date,
    feature_name String,
    usage_count UInt32,
    usage_duration_secs UInt32,
    error_count UInt16,
    is_beta_feature Bool
)
ENGINE = MergeTree
ORDER BY (subscription_id, usage_date);

CREATE TABLE staging.support_tickets
(
    ticket_id String,
    account_id String,
    submitted_at DateTime,
    closed_at DateTime,
    resolution_time_hours Float32,
    priority String,
    first_response_time_minutes UInt32,
    satisfaction_score Nullable(UInt8),
    escalation_flag Bool
)
ENGINE = MergeTree
ORDER BY submitted_at;

CREATE TABLE staging.churn_events
(
    churn_event_id String,
    account_id String,
    churn_date Date,
    reason_code String,
    refund_amount_usd Float32,
    preceding_upgrade_flag Bool,
    preceding_downgrade_flag Bool,
    is_reactivation Bool,
    feedback_text String
)
ENGINE = MergeTree
ORDER BY churn_date;