"""
load_warehouse_incremental.py
==============================
Incremental loading from ClickHouse staging → production dims & facts.

Pattern
-------
- Uses ReplacingMergeTree(ver) engine with _updated_at (epoch milliseconds).
- Insert-only: new rows always win because they carry a higher _updated_at.
- Dims are loaded BEFORE facts (FK dependency order).
- Late-arriving data is handled automatically — the engine keeps the latest
  version of each PK.
- Use SELECT FINAL in downstream queries (marts/BI) to read deduplicated rows.

Load order
----------
1. production.dim_accounts      (no deps)
2. production.dim_plans         (static seed — nothing to do)
3. production.fact_subscriptions (depends on dim_accounts, dim_plans)
4. production.fact_churn_events  (depends on dim_accounts)
5. production.fact_feature_usage (depends on fact_subscriptions)
6. production.fact_support_tickets (depends on dim_accounts)
"""

import os
import logging
import time
from datetime import date, datetime, timedelta

import clickhouse_connect

from watermark_tracker import get_watermark, set_watermark

# ===========================================================
# LOGGING
# ===========================================================
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# ===========================================================
# CONFIGURATION
# ===========================================================
CH_HOST     = os.getenv("CH_HOST",     "clickhouse-saas")
CH_PORT     = int(os.getenv("CH_PORT", "8123"))
CH_USER     = os.getenv("CH_USER",     "default")
CH_PASSWORD = os.getenv("CH_PASSWORD", "password123")

WM_PREFIX = "warehouse_"


# ===========================================================
# ClickHouse Client
# ===========================================================

def get_ch_client() -> clickhouse_connect.driver.Client:
    for attempt in range(5):
        try:
            return clickhouse_connect.get_client(
                host=CH_HOST, port=CH_PORT,
                username=CH_USER, password=CH_PASSWORD,
            )
        except Exception as exc:
            log.warning(f"ClickHouse not ready (attempt {attempt+1}/5): {exc}")
            time.sleep(3)
    raise ConnectionError("Could not connect to ClickHouse after 5 attempts")


# ===========================================================
# INCREMENTAL LOAD HELPER
# ===========================================================

def load_incremental(
    ch: clickhouse_connect.driver.Client,
    source_table: str,
    target_table: str,
    pk: str,
    date_col: str,
    watermark: date,
    load_date: date,
    extra_select: str = "",
) -> int:
    """
    Copy new/updated rows from *source_table* to *target_table*.

    Rows are filtered by date_col in (watermark, load_date].
    _updated_at is set to current epoch ms so newer inserts always win.

    Returns the number of rows inserted.
    """
    updated_at_ms = int(datetime.utcnow().timestamp() * 1000)

    query = f"""
    INSERT INTO {target_table}
    SELECT
        *,
        {updated_at_ms} AS _updated_at
    {extra_select}
    FROM {source_table}
    WHERE toDate({date_col}) > toDate('{watermark}')
      AND toDate({date_col}) <= toDate('{load_date}')
    """

    # Get count before insert
    count_q = f"""
    SELECT count()
    FROM {source_table}
    WHERE toDate({date_col}) > toDate('{watermark}')
      AND toDate({date_col}) <= toDate('{load_date}')
    """
    result = ch.query(count_q)
    n_rows = result.result_rows[0][0] if result.result_rows else 0

    if n_rows == 0:
        log.info(f"{source_table} → {target_table}: 0 new rows — skipping")
        return 0

    ch.command(query)
    log.info(f"{source_table} → {target_table}: inserted {n_rows:,} rows")
    return n_rows


# ===========================================================
# DIMENSION LOADERS
# ===========================================================

def load_dim_accounts(ch, watermark: date, load_date: date) -> int:
    return load_incremental(
        ch,
        source_table="staging.stg_accounts",
        target_table="production.dim_accounts",
        pk="account_id",
        date_col="signup_date",
        watermark=watermark,
        load_date=load_date,
        # stg_accounts has no _updated_at column; add it via SELECT
        extra_select=""  # covered by load_incremental's _updated_at injection
    )


# ===========================================================
# FACT LOADERS
# ===========================================================

def load_fact_subscriptions(ch, watermark: date, load_date: date) -> int:
    """
    Loads subscriptions + derived metrics (subscription_sequence, days_active)
    via a SQL window function expressed as ClickHouse-compatible aggregation.
    Note: full window functions are computed in Python transform step;
    here we simply copy the already-enriched staging rows.
    """
    return load_incremental(
        ch,
        source_table="staging.stg_subscriptions",
        target_table="production.fact_subscriptions",
        pk="subscription_id",
        date_col="start_date",
        watermark=watermark,
        load_date=load_date,
    )


def load_fact_churn_events(ch, watermark: date, load_date: date) -> int:
    return load_incremental(
        ch,
        source_table="staging.stg_churn_events",
        target_table="production.fact_churn_events",
        pk="churn_event_id",
        date_col="churn_date",
        watermark=watermark,
        load_date=load_date,
    )


def load_fact_feature_usage(ch, watermark: date, load_date: date) -> int:
    return load_incremental(
        ch,
        source_table="staging.stg_feature_usage",
        target_table="production.fact_feature_usage",
        pk="usage_id",
        date_col="usage_date",
        watermark=watermark,
        load_date=load_date,
    )


def load_fact_support_tickets(ch, watermark: date, load_date: date) -> int:
    return load_incremental(
        ch,
        source_table="staging.stg_support_tickets",
        target_table="production.fact_support_tickets",
        pk="ticket_id",
        date_col="submitted_at",
        watermark=watermark,
        load_date=load_date,
    )


# ===========================================================
# MAIN ORCHESTRATION
# ===========================================================

LOAD_STEPS = [
    # (name, loader_fn, table_key)
    ("dim_accounts",          load_dim_accounts,          "dim_accounts"),
    ("fact_subscriptions",    load_fact_subscriptions,    "fact_subscriptions"),
    ("fact_churn_events",     load_fact_churn_events,     "fact_churn_events"),
    ("fact_feature_usage",    load_fact_feature_usage,    "fact_feature_usage"),
    ("fact_support_tickets",  load_fact_support_tickets,  "fact_support_tickets"),
]


def run_warehouse_load(load_date: date | None = None) -> None:
    if load_date is None:
        load_date = date.today() - timedelta(days=1)

    log.info("=" * 60)
    log.info(f"INCREMENTAL WAREHOUSE LOAD  |  load_date={load_date}")
    log.info("=" * 60)

    ch = get_ch_client()
    summary = {}

    for name, loader_fn, wm_key in LOAD_STEPS:
        full_key  = WM_PREFIX + wm_key
        watermark = get_watermark(full_key)

        log.info(f"\n[{name}] watermark={watermark} → load_date={load_date}")

        try:
            n = loader_fn(ch, watermark, load_date)
            set_watermark(full_key, load_date)
            summary[name] = {"status": "ok", "rows": n}
        except Exception as exc:
            log.error(f"[{name}] FAILED: {exc}", exc_info=True)
            summary[name] = {"status": "error", "error": str(exc)}

    log.info("\n" + "=" * 60)
    log.info("WAREHOUSE LOAD SUMMARY")
    log.info("=" * 60)
    for tbl, result in summary.items():
        if result["status"] == "ok":
            log.info(f"  {tbl:<30} OK   ({result['rows']:>8,} rows)")
        else:
            log.error(f"  {tbl:<30} FAIL  {result['error']}")


# ===========================================================
# ENTRY POINT
# ===========================================================
if __name__ == "__main__":
    import sys
    override_date = None
    if len(sys.argv) == 2:
        override_date = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
    run_warehouse_load(override_date)
