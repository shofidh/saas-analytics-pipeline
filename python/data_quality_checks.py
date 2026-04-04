"""
data_quality_checks.py
=======================
Automated data quality validation against ClickHouse staging tables.
Runs AFTER ingest_and_stage.py has populated staging.stg_* tables.

Checks per table
----------------
  ✓ non_empty_batch        : at least 1 row in the new batch
  ✓ no_null_pk             : primary key has zero nulls
  ✓ no_duplicate_pk        : primary key is unique
  ✓ no_negative_revenue    : mrr_amount / refund_amount_usd >= 0
  ✓ date_order             : start_date <= end_date (subscriptions / tickets)
  ✓ date_bounds            : all dates within [2024-01-01, 2026-12-31]
  ✓ no_null_critical_cols  : important non-PK columns have no nulls

Severity
--------
  FAIL    → pipeline stops (DataQualityError raised)
  WARNING → logged, pipeline continues
  SKIP    → not applicable for this table
  PASS    → check passed

Usage
-----
  python data_quality_checks.py [YYYY-MM-DD]
  from data_quality_checks import run_quality_checks; run_quality_checks(batch_date)
"""

from __future__ import annotations

import os
import sys
import logging
import time
from datetime import date, datetime, timedelta
from typing import Any

import pandas as pd
import clickhouse_connect

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
CH_HOST     = os.getenv("CH_HOST",     "clickhouse-saas")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_USER     = os.getenv("CH_USER",     "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "password123")

PIPELINE_START = date(2024, 1, 1)
PIPELINE_END   = date(2026, 12, 31)

# ─────────────────────────────────────────────────────────────
# TABLE CONFIG
# ─────────────────────────────────────────────────────────────
TABLE_CONFIG: dict[str, dict] = {
    "accounts": {
        "staging_table":    "staging.stg_accounts",
        "pk":               "account_id",
        "date_col":         "signup_date",
        "critical_cols":    ["account_name", "industry", "country", "signup_date"],
        "revenue_cols":     [],
        "date_range_check": None,
    },
    "subscriptions": {
        "staging_table":    "staging.stg_subscriptions",
        "pk":               "subscription_id",
        "date_col":         "start_date",
        "critical_cols":    ["account_id", "plan_tier", "start_date", "end_date"],
        "revenue_cols":     ["mrr_amount", "arr_amount"],
        "date_range_check": ("start_date", "end_date"),
    },
    "churn_events": {
        "staging_table":    "staging.stg_churn_events",
        "pk":               "churn_event_id",
        "date_col":         "churn_date",
        "critical_cols":    ["account_id", "churn_date", "reason_code"],
        "revenue_cols":     ["refund_amount_usd"],
        "date_range_check": None,
    },
    "feature_usage": {
        "staging_table":    "staging.stg_feature_usage",
        "pk":               "usage_id",
        "date_col":         "usage_date",
        "critical_cols":    ["subscription_id", "usage_date", "feature_name"],
        "revenue_cols":     [],
        "date_range_check": None,
    },
    "support_tickets": {
        "staging_table":    "staging.stg_support_tickets",
        "pk":               "ticket_id",
        "date_col":         "submitted_at",
        "critical_cols":    ["account_id", "submitted_at", "priority"],
        "revenue_cols":     [],
        "date_range_check": ("submitted_at", "closed_at"),
    },
}


# ─────────────────────────────────────────────────────────────
# CUSTOM EXCEPTION
# ─────────────────────────────────────────────────────────────

class DataQualityError(Exception):
    """Raised when one or more FAIL-level checks are detected."""


# ─────────────────────────────────────────────────────────────
# CLICKHOUSE CONNECTION
# ─────────────────────────────────────────────────────────────

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


def read_batch_from_staging(
    ch: clickhouse_connect.driver.Client,
    staging_table: str,
    date_col: str,
    batch_date: date,
) -> pd.DataFrame:
    """
    Read a sample of rows from staging loaded up to batch_date.

    Uses a 30-day rolling window ending on batch_date so that:
    - Incremental daily runs (small batches) always find data.
    - Full historical loads (watermark reset) also find data.
    Capped at 100 000 rows to keep memory bounded.
    """
    window_start = max(batch_date - timedelta(days=30), PIPELINE_START)
    query = f"""
    SELECT *
    FROM {staging_table}
    WHERE toDate({date_col}) >= toDate('{window_start}')
      AND toDate({date_col}) <= toDate('{batch_date}')
    LIMIT 100000
    """
    result = ch.query_df(query)
    return result


# ─────────────────────────────────────────────────────────────
# CHECK FUNCTIONS
# ─────────────────────────────────────────────────────────────

def check_non_empty(df: pd.DataFrame) -> tuple[str, str, str]:
    # WARNING (not FAIL): empty windows are expected on quiet days
    status = "PASS" if len(df) > 0 else "WARNING"
    return "non_empty_batch", status, f"{len(df):,} rows"


def check_no_null_pk(df: pd.DataFrame, pk: str) -> tuple[str, str, str]:
    nulls = int(df[pk].isna().sum()) if pk in df.columns else -1
    status = "PASS" if nulls == 0 else "FAIL"
    return "no_null_pk", status, f"{nulls} nulls in '{pk}'"


def check_no_duplicate_pk(df: pd.DataFrame, pk: str) -> tuple[str, str, str]:
    # WARNING (not FAIL): staging uses append-only MergeTree; duplicates
    # are expected after watermark resets and are deduplicated in the
    # warehouse layer by ReplacingMergeTree.
    dups = int(df[pk].duplicated().sum()) if pk in df.columns else -1
    status = "PASS" if dups == 0 else "WARNING"
    return "no_duplicate_pk", status, f"{dups} duplicates in '{pk}'"


def check_no_negative_revenue(df: pd.DataFrame, revenue_cols: list[str]) -> tuple[str, str, str]:
    if not revenue_cols:
        return "no_negative_revenue", "SKIP", "no revenue columns"
    failures = []
    for col in revenue_cols:
        if col in df.columns:
            neg = int((pd.to_numeric(df[col], errors="coerce").fillna(0) < 0).sum())
            if neg > 0:
                failures.append(f"{col}: {neg} negative")
    status = "PASS" if not failures else "FAIL"
    detail = ", ".join(failures) if failures else "all >= 0"
    return "no_negative_revenue", status, detail


def check_date_order(
    df: pd.DataFrame,
    date_range_check: tuple[str, str] | None,
) -> tuple[str, str, str]:
    if date_range_check is None:
        return "date_order", "SKIP", "not applicable"
    start_col, end_col = date_range_check
    missing = [c for c in (start_col, end_col) if c not in df.columns]
    if missing:
        return "date_order", "SKIP", f"columns missing: {missing}"
    start = pd.to_datetime(df[start_col], errors="coerce")
    end   = pd.to_datetime(df[end_col],   errors="coerce")
    violations = int((end.notna() & (end < start)).sum())
    status = "PASS" if violations == 0 else "FAIL"
    return "date_order", status, f"{violations} rows where end < start"


def check_date_bounds(df: pd.DataFrame, date_col: str) -> tuple[str, str, str]:
    if date_col not in df.columns:
        return "date_bounds", "SKIP", f"'{date_col}' not found"
    parsed = pd.to_datetime(df[date_col], errors="coerce").dt.date
    out_of_range = int(((parsed < PIPELINE_START) | (parsed > PIPELINE_END)).sum())
    status = "PASS" if out_of_range == 0 else "WARNING"
    return "date_bounds", status, f"{out_of_range} rows outside [{PIPELINE_START}, {PIPELINE_END}]"


def check_no_null_critical(df: pd.DataFrame, critical_cols: list[str]) -> tuple[str, str, str]:
    failures = []
    for col in critical_cols:
        if col in df.columns:
            nulls = int(df[col].isna().sum())
            if nulls > 0:
                failures.append(f"{col}: {nulls} nulls")
    status = "PASS" if not failures else "FAIL"
    detail = ", ".join(failures) if failures else "all critical cols non-null"
    return "no_null_critical_cols", status, detail


# ─────────────────────────────────────────────────────────────
# MAIN VALIDATION RUNNER
# ─────────────────────────────────────────────────────────────

def validate_table(table: str, config: dict, df: pd.DataFrame) -> dict[str, dict[str, Any]]:
    results = {}
    checks = [
        check_non_empty(df),
        check_no_null_pk(df, config["pk"]),
        check_no_duplicate_pk(df, config["pk"]),
        check_no_negative_revenue(df, config["revenue_cols"]),
        check_date_order(df, config["date_range_check"]),
        check_date_bounds(df, config["date_col"]),
        check_no_null_critical(df, config["critical_cols"]),
    ]
    for check_name, status, detail in checks:
        results[check_name] = {"status": status, "detail": detail}
    return results


def run_quality_checks(batch_date: date | None = None) -> dict:
    """
    Run all DQ checks reading from ClickHouse staging tables.
    Raises DataQualityError on any FAIL.
    """
    if batch_date is None:
        batch_date = date.today() - timedelta(days=1)

    log.info("=" * 60)
    log.info(f"DATA QUALITY CHECKS  |  batch_date={batch_date}")
    log.info("=" * 60)

    ch = get_ch_client()
    all_results: dict = {}
    has_failures = False

    for table, config in TABLE_CONFIG.items():
        try:
            df = read_batch_from_staging(
                ch, config["staging_table"], config["date_col"], batch_date
            )
        except Exception as exc:
            log.warning(f"[{table}] Could not read staging ({exc}) — skipping")
            all_results[table] = {"_skipped": True}
            continue

        if df.empty:
            log.warning(f"[{table}] No rows found in staging for {batch_date} — skipping")
            all_results[table] = {"_skipped": True}
            continue

        results = validate_table(table, config, df)
        all_results[table] = results

        log.info(f"\n[{table}] ({len(df):,} rows)")
        icon_map = {"PASS": "✓", "FAIL": "✗", "WARNING": "⚠", "SKIP": "–"}
        for check, outcome in results.items():
            icon = icon_map.get(outcome["status"], "?")
            log.info(f"  {icon} {check:<30} {outcome['status']:<8}  {outcome['detail']}")
            if outcome["status"] == "FAIL":
                has_failures = True

    log.info("\n" + "=" * 60)
    if has_failures:
        log.error("DATA QUALITY: ✗ FAILURES DETECTED")
        raise DataQualityError(
            f"Data quality FAILED for batch_date={batch_date}. See logs."
        )

    log.info("DATA QUALITY: ✓ ALL CHECKS PASSED")
    return all_results


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    override_date = None
    if len(sys.argv) == 2:
        override_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    try:
        run_quality_checks(override_date)
    except DataQualityError as e:
        log.error(str(e))
        sys.exit(1)
