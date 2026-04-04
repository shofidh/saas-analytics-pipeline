"""
transform_incremental.py
=========================
Incremental transformation: MinIO (Parquet) → ClickHouse staging tables.

Flow (per table, per daily run):
  MinIO: saas-raw/{table}/year=YYYY/month=MM/day=DD/part-0.parquet
      → read partitions where date > transform_watermark AND <= batch_date
      → apply business logic (churn_flag, reactivation_flag, MRR/ARR, etc.)
      → INSERT into ClickHouse staging.stg_{table}
      → advance transform watermark

Design principles:
  - Reads ONLY from MinIO (never touches raw CSVs directly)
  - Applies domain-specific enrichment per table
  - Idempotent: re-running inserts with same data; ReplacingMergeTree deduplicates
  - Incremental: only transforms new partitions since last run

Business logic applied:
  accounts        → boolean normalization, type casting
  subscriptions   → subscription_sequence, days_active, churn_flag (gap>60d),
                     reactivation_flag (gap>60d), MRR/ARR validation
  churn_events    → reason_code normalization, refund clipping
  feature_usage   → usage count/duration clipping, beta flag
  support_tickets → resolution time normalization, satisfaction clipping
"""

from __future__ import annotations

import io
import os
import logging
import time
from datetime import date, datetime, timedelta

import pandas as pd
import pyarrow.parquet as pq
import clickhouse_connect
from minio import Minio

from watermark_tracker import get_watermark, set_watermark

# ─────────────────────────────────────────────────────────────
# LOGGING
# ─────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────
CH_HOST          = os.getenv("CH_HOST",          "clickhouse-saas")
CH_PORT          = int(os.getenv("CH_PORT",      "8123"))
CH_USER          = os.getenv("CH_USER",          "default")
CH_PASSWORD      = os.getenv("CH_PASSWORD",      "password123")

MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio-saas:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "password123")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",      "saas-raw")
MINIO_SECURE     = os.getenv("MINIO_SECURE",      "false").lower() == "true"

CHURN_GAP_DAYS        = 60
REACTIVATION_GAP_DAYS = 60

# ─────────────────────────────────────────────────────────────
# TABLE CONFIGURATION
# ─────────────────────────────────────────────────────────────
TABLE_CONFIG: dict[str, dict] = {
    "accounts": {
        "pk":            "account_id",
        "date_col":      "signup_date",
        "staging_table": "staging.stg_accounts",
    },
    "subscriptions": {
        "pk":            "subscription_id",
        "date_col":      "start_date",
        "staging_table": "staging.stg_subscriptions",
    },
    "churn_events": {
        "pk":            "churn_event_id",
        "date_col":      "churn_date",
        "staging_table": "staging.stg_churn_events",
    },
    "feature_usage": {
        "pk":            "usage_id",
        "date_col":      "usage_date",
        "staging_table": "staging.stg_feature_usage",
    },
    "support_tickets": {
        "pk":            "ticket_id",
        "date_col":      "submitted_at",
        "staging_table": "staging.stg_support_tickets",
    },
}


# ─────────────────────────────────────────────────────────────
# CLIENTS
# ─────────────────────────────────────────────────────────────

def get_minio_client() -> Minio:
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )


def get_ch_client() -> clickhouse_connect.driver.Client:
    for attempt in range(1, 6):
        try:
            client = clickhouse_connect.get_client(
                host=CH_HOST, port=CH_PORT,
                username=CH_USER, password=CH_PASSWORD,
            )
            client.command("SELECT 1")
            return client
        except Exception as exc:
            log.warning(f"ClickHouse not ready (attempt {attempt}/5): {exc}")
            time.sleep(5)
    raise ConnectionError("Could not connect to ClickHouse after 5 attempts")


# ─────────────────────────────────────────────────────────────
# MINIO READER — Read partitions for a date range
# ─────────────────────────────────────────────────────────────

def _list_partition_objects(
    client: Minio,
    table: str,
    from_date: date,
    to_date: date,
) -> list[str]:
    """
    List all Parquet object keys in saas-raw/{table}/year=.../month=.../day=.../
    that fall in the half-open range (from_date, to_date].
    """
    prefix = f"{table}/"
    objects = client.list_objects(MINIO_BUCKET, prefix=prefix, recursive=True)

    matching = []
    for obj in objects:
        key = obj.object_name
        if not key.endswith(".parquet"):
            continue

        # Parse partition date from path: {table}/year=YYYY/month=MM/day=DD/...
        try:
            parts = key.split("/")
            year_part  = [p for p in parts if p.startswith("year=")][0]
            month_part = [p for p in parts if p.startswith("month=")][0]
            day_part   = [p for p in parts if p.startswith("day=")][0]
            part_date  = date(
                int(year_part.split("=")[1]),
                int(month_part.split("=")[1]),
                int(day_part.split("=")[1]),
            )
        except (IndexError, ValueError):
            log.warning(f"  Could not parse date from: {key} — skipping")
            continue

        # Half-open interval: (from_date, to_date]
        if from_date < part_date <= to_date:
            matching.append(key)

    return sorted(matching)


def read_partitions_from_minio(
    client: Minio,
    table: str,
    from_date: date,
    to_date: date,
) -> pd.DataFrame:
    """
    Read and concatenate all Parquet files for *table* in (from_date, to_date].
    Returns an empty DataFrame if no partitions found.
    """
    keys = _list_partition_objects(client, table, from_date, to_date)
    if not keys:
        return pd.DataFrame()

    frames = []
    for key in keys:
        response = client.get_object(MINIO_BUCKET, key)
        buf = io.BytesIO(response.read())
        response.close()
        response.release_conn()

        parquet_table = pq.read_table(buf)
        frames.append(parquet_table.to_pandas())

    df = pd.concat(frames, ignore_index=True)
    log.info(f"  Read {len(keys)} partitions → {len(df):,} rows from MinIO")
    return df


# ─────────────────────────────────────────────────────────────
# BUSINESS LOGIC — PER-TABLE ENRICHMENT
# ─────────────────────────────────────────────────────────────

def enrich_accounts(df: pd.DataFrame) -> pd.DataFrame:
    df["signup_date"] = pd.to_datetime(df["signup_date"], errors="coerce").dt.date
    df["churn_flag"]  = df["churn_flag"].fillna(False).astype(bool)
    df["is_trial"]    = df["is_trial"].fillna(False).astype(bool)
    return df


def enrich_subscriptions(df: pd.DataFrame) -> pd.DataFrame:
    df["start_date"] = pd.to_datetime(df["start_date"], errors="coerce")
    df["end_date"]   = pd.to_datetime(df["end_date"],   errors="coerce")

    # Sort for window operations
    df = df.sort_values(["account_id", "start_date"]).reset_index(drop=True)

    # Gap analysis helpers
    df["_next_start"] = df.groupby("account_id")["start_date"].shift(-1)
    df["_prev_end"]   = df.groupby("account_id")["end_date"].shift(1)

    gap_to_next   = (df["_next_start"] - df["end_date"]).dt.days
    gap_from_prev = (df["start_date"]  - df["_prev_end"]).dt.days

    # churn_flag: no renewal within gap threshold, or last known subscription
    df["churn_flag"] = gap_to_next.isna() | (gap_to_next > CHURN_GAP_DAYS)

    # reactivation_flag: previous gap > threshold (returning customer)
    df["reactivation_flag"] = gap_from_prev.notna() & (gap_from_prev > REACTIVATION_GAP_DAYS)

    # subscription_sequence: chronological rank per account (1 = first subscription)
    df["subscription_sequence"] = (
        df.groupby("account_id")["start_date"]
        .rank(method="first")
        .astype(int)
    )

    # days_active: duration of subscription in days
    df["days_active"] = (
        (df["end_date"] - df["start_date"]).dt.days
        .clip(lower=0).fillna(0).astype(int)
    )

    # MRR / ARR
    df["mrr_amount"] = df["mrr_amount"].clip(lower=0).fillna(0).round(2)
    df["arr_amount"] = (df["mrr_amount"] * 12).round(2)

    df.drop(columns=["_next_start", "_prev_end"], inplace=True)
    return df


def enrich_churn_events(df: pd.DataFrame) -> pd.DataFrame:
    df["churn_date"]        = pd.to_datetime(df["churn_date"], errors="coerce").dt.date
    df["refund_amount_usd"] = df["refund_amount_usd"].clip(lower=0).fillna(0).round(2)
    df["reason_code"]       = df["reason_code"].fillna("unknown")
    return df


def enrich_feature_usage(df: pd.DataFrame) -> pd.DataFrame:
    df["usage_date"]          = pd.to_datetime(df["usage_date"], errors="coerce").dt.date
    df["usage_count"]         = df["usage_count"].clip(lower=0).fillna(0).astype(int)
    df["usage_duration_secs"] = df["usage_duration_secs"].clip(lower=0).fillna(0).astype(int)
    df["error_count"]         = df["error_count"].clip(lower=0).fillna(0).astype(int)
    df["is_beta_feature"]     = df["is_beta_feature"].fillna(False).astype(bool)
    return df


def enrich_support_tickets(df: pd.DataFrame) -> pd.DataFrame:
    df["submitted_at"]               = pd.to_datetime(df["submitted_at"], errors="coerce")
    df["closed_at"]                  = pd.to_datetime(df["closed_at"],    errors="coerce")
    df["resolution_time_hours"]      = df["resolution_time_hours"].clip(lower=0).fillna(0)
    df["first_response_time_minutes"]= df["first_response_time_minutes"].clip(lower=0).fillna(0)
    df["satisfaction_score"]         = df["satisfaction_score"].clip(lower=1, upper=5)
    df["escalation_flag"]            = df["escalation_flag"].fillna(False).astype(bool)
    return df


ENRICH_MAP = {
    "accounts":        enrich_accounts,
    "subscriptions":   enrich_subscriptions,
    "churn_events":    enrich_churn_events,
    "feature_usage":   enrich_feature_usage,
    "support_tickets": enrich_support_tickets,
}


# ─────────────────────────────────────────────────────────────
# CLICKHOUSE INSERT HELPER
# ─────────────────────────────────────────────────────────────

def insert_to_staging(
    ch: clickhouse_connect.driver.Client,
    table: str,
    df: pd.DataFrame,
) -> None:
    """Insert DataFrame into a ClickHouse staging table."""
    if df.empty:
        log.info(f"  → {table}: nothing to insert")
        return

    # Convert bool → uint8 for ClickHouse compatibility
    for col in df.select_dtypes(include="bool").columns:
        df[col] = df[col].astype("uint8")

    ch.insert_df(table, df)
    log.info(f"  → {table}: inserted {len(df):,} rows")


# ─────────────────────────────────────────────────────────────
# COLUMN ALIGNMENT — ensure DataFrame matches staging schema
# ─────────────────────────────────────────────────────────────

# Expected column order per staging table (must match DDL exactly)
STAGING_COLUMNS = {
    "accounts": [
        "account_id", "account_name", "industry", "country", "signup_date",
        "referral_source", "plan_tier", "seats", "is_trial", "churn_flag",
        "_ingestion_time", "_source_system", "_batch_id",
    ],
    "subscriptions": [
        "subscription_id", "account_id", "start_date", "end_date", "plan_tier",
        "seats", "price", "discount_value", "mrr_amount", "arr_amount",
        "is_trial", "upgrade_flag", "downgrade_flag", "churn_flag",
        "reactivation_flag", "billing_frequency", "auto_renew_flag",
        "subscription_sequence", "days_active",
        "_ingestion_time", "_source_system", "_batch_id",
    ],
    "churn_events": [
        "churn_event_id", "account_id", "churn_date", "reason_code",
        "refund_amount_usd", "preceding_upgrade_flag", "preceding_downgrade_flag",
        "is_reactivation", "feedback_text",
        "_ingestion_time", "_source_system", "_batch_id",
    ],
    "feature_usage": [
        "usage_id", "subscription_id", "usage_date", "feature_name",
        "usage_count", "usage_duration_secs", "error_count", "is_beta_feature",
        "_ingestion_time", "_source_system", "_batch_id",
    ],
    "support_tickets": [
        "ticket_id", "account_id", "submitted_at", "closed_at",
        "resolution_time_hours", "priority", "first_response_time_minutes",
        "satisfaction_score", "escalation_flag",
        "_ingestion_time", "_source_system", "_batch_id",
    ],
}


def align_columns(df: pd.DataFrame, table: str) -> pd.DataFrame:
    """
    Ensure DataFrame has exactly the columns expected by the staging table,
    in the correct order. Drops extra columns, adds missing ones as NULL.
    """
    expected = STAGING_COLUMNS.get(table, [])
    if not expected:
        return df

    # Drop columns that aren't in the staging schema
    extra = [c for c in df.columns if c not in expected]
    if extra:
        log.info(f"  Dropping extra columns: {extra}")
        df = df.drop(columns=extra)

    # Add missing columns as None
    missing = [c for c in expected if c not in df.columns]
    for col in missing:
        df[col] = None

    return df[expected]


# ─────────────────────────────────────────────────────────────
# PER-TABLE PIPELINE
# ─────────────────────────────────────────────────────────────

def transform_table(
    table: str,
    cfg: dict,
    minio_client: Minio,
    ch: clickhouse_connect.driver.Client,
    batch_date: date,
) -> int:
    """
    Read new partitions from MinIO, apply business logic, insert into staging.
    Returns number of rows loaded.
    """
    wm_key    = f"transform_{table}"
    watermark = get_watermark(wm_key)
    log.info(f"[{table}] transform watermark={watermark}  →  batch_date={batch_date}")

    # 1. Read new partitions from MinIO
    df = read_partitions_from_minio(minio_client, table, watermark, batch_date)

    if df.empty:
        log.info(f"[{table}] 0 new rows in MinIO — nothing to transform")
        return 0

    # 2. Deduplicate on PK
    pk = cfg["pk"]
    before = len(df)
    df.drop_duplicates(subset=[pk], keep="last", inplace=True)
    if len(df) < before:
        log.warning(f"[{table}] dropped {before - len(df)} duplicates on {pk}")

    # 3. Apply domain-specific business logic
    enrich_fn = ENRICH_MAP.get(table)
    if enrich_fn:
        df = enrich_fn(df)

    # 4. Align columns to staging schema
    df = align_columns(df, table)

    # 5. Insert into ClickHouse staging
    insert_to_staging(ch, cfg["staging_table"], df)

    # 6. Advance transform watermark
    set_watermark(wm_key, batch_date)
    log.info(f"[{table}] transform watermark advanced to {batch_date}")

    return len(df)


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def run_transform(batch_date: date | None = None) -> dict:
    """
    Run incremental transformation for all tables.
    Returns summary dict.
    """
    if batch_date is None:
        batch_date = date.today() - timedelta(days=1)

    log.info("=" * 60)
    log.info(f"INCREMENTAL TRANSFORM  |  batch_date={batch_date}")
    log.info("=" * 60)

    minio_client = get_minio_client()
    ch           = get_ch_client()
    summary: dict[str, dict] = {}

    for table, cfg in TABLE_CONFIG.items():
        try:
            n = transform_table(table, cfg, minio_client, ch, batch_date)
            summary[table] = {"status": "ok", "rows": n}
        except Exception as exc:
            log.error(f"[{table}] FAILED: {exc}", exc_info=True)
            summary[table] = {"status": "error", "error": str(exc)}

    log.info("\n" + "=" * 60)
    log.info("TRANSFORM SUMMARY")
    log.info("=" * 60)
    for tbl, res in summary.items():
        if res["status"] == "ok":
            log.info(f"  {tbl:<25} OK    ({res['rows']:>8,} rows)")
        else:
            log.error(f"  {tbl:<25} FAIL  {res['error']}")

    failed = [t for t, r in summary.items() if r["status"] == "error"]
    if failed:
        raise RuntimeError(f"Transform failed for tables: {failed}")

    return summary


if __name__ == "__main__":
    import sys
    override = None
    if len(sys.argv) == 2:
        override = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        log.info(f"CLI override: batch_date={override}")
    run_transform(override)
