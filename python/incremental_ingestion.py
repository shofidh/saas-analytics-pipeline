"""
incremental_ingestion.py
=========================
Incremental data ingestion: local CSV → MinIO (S3-compatible) as partitioned Parquet.

Flow (per table, per daily batch):
  data/raw_data/{table}.csv
      → filter rows where date_col > watermark AND date_col <= batch_date
      → deduplicate on primary key
      → add metadata: _ingestion_time, _source_system, _batch_id
      → write as Parquet → upload to MinIO
      → advance watermark

MinIO partition layout:
  s3://saas-raw/{table}/year=YYYY/month=MM/day=DD/part-0.parquet

Design principles:
  - Idempotent:  re-running the same batch_date overwrites the same partition
  - Incremental: reads only new rows since last successful run
  - Immutable:   once uploaded, raw data is never mutated in the lake

Source systems:
  accounts        → crm_system
  subscriptions   → transactional_system
  churn_events    → transactional_system
  feature_usage   → application_logs
  support_tickets → support_system
"""

from __future__ import annotations

import io
import os
import logging
import uuid
from datetime import date, datetime, timedelta

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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
RAW_DATA_DIR     = os.getenv("RAW_DATA_DIR",     "/app/data/raw_data")
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT",   "minio-saas:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY",  "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY",  "password123")
MINIO_BUCKET     = os.getenv("MINIO_BUCKET",      "saas-raw")
MINIO_SECURE     = os.getenv("MINIO_SECURE",      "false").lower() == "true"

# ─────────────────────────────────────────────────────────────
# TABLE CONFIGURATION
# ─────────────────────────────────────────────────────────────
TABLE_CONFIG: dict[str, dict] = {
    "accounts": {
        "file":          "accounts.csv",
        "pk":            "account_id",
        "date_col":      "signup_date",
        "source_system": "crm_system",
    },
    "subscriptions": {
        "file":          "subscriptions.csv",
        "pk":            "subscription_id",
        "date_col":      "start_date",
        "source_system": "transactional_system",
    },
    "churn_events": {
        "file":          "churn_events.csv",
        "pk":            "churn_event_id",
        "date_col":      "churn_date",
        "source_system": "transactional_system",
    },
    "feature_usage": {
        "file":          "feature_usage.csv",
        "pk":            "usage_id",
        "date_col":      "usage_date",
        "source_system": "application_logs",
    },
    "support_tickets": {
        "file":          "support_tickets.csv",
        "pk":            "ticket_id",
        "date_col":      "submitted_at",
        "source_system": "support_system",
    },
}


# ─────────────────────────────────────────────────────────────
# MINIO CLIENT
# ─────────────────────────────────────────────────────────────

def get_minio_client() -> Minio:
    """Create and return a MinIO client; ensure the bucket exists."""
    client = Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=MINIO_SECURE,
    )
    if not client.bucket_exists(MINIO_BUCKET):
        client.make_bucket(MINIO_BUCKET)
        log.info(f"Created bucket: {MINIO_BUCKET}")
    return client


# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def standardise_columns(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    """Lowercase columns, strip whitespace, parse date column."""
    df = df.copy()
    df.columns = [c.strip().lower() for c in df.columns]
    df[date_col] = pd.to_datetime(df[date_col], errors="coerce").dt.date
    for col in df.columns:
        if df[col].dtype == "object":
            df[col] = df[col].apply(lambda x: x.strip() if isinstance(x, str) else x)
    return df


def upload_parquet_to_minio(
    client: Minio,
    table: str,
    df: pd.DataFrame,
    partition_date: date,
) -> str:
    """
    Convert DataFrame to Parquet bytes and upload to MinIO.
    Returns the object path.

    Path pattern: {table}/year=YYYY/month=MM/day=DD/part-0.parquet
    """
    year  = str(partition_date.year)
    month = f"{partition_date.month:02d}"
    day   = f"{partition_date.day:02d}"

    object_path = f"{table}/year={year}/month={month}/day={day}/part-0.parquet"

    # Convert to Parquet in memory
    arrow_table = pa.Table.from_pandas(df, preserve_index=False)
    buf = io.BytesIO()
    pq.write_table(arrow_table, buf, compression="snappy")
    buf.seek(0)
    buf_size = buf.getbuffer().nbytes

    # Upload (overwrites existing = idempotent)
    client.put_object(
        MINIO_BUCKET,
        object_path,
        data=buf,
        length=buf_size,
        content_type="application/octet-stream",
    )

    log.info(f"  → MinIO: s3://{MINIO_BUCKET}/{object_path} ({buf_size:,} bytes)")
    return object_path


# ─────────────────────────────────────────────────────────────
# PER-TABLE PIPELINE
# ─────────────────────────────────────────────────────────────

def ingest_table(
    table: str,
    cfg: dict,
    minio_client: Minio,
    batch_date: date,
) -> int:
    """
    Read new rows from CSV, add metadata, upload to MinIO as Parquet.
    Returns number of rows ingested.
    """
    wm_key    = f"ingestion_{table}"
    watermark = get_watermark(wm_key)
    log.info(f"[{table}] watermark={watermark}  →  batch_date={batch_date}")

    # 1. Read CSV
    csv_path = os.path.join(RAW_DATA_DIR, cfg["file"])
    if not os.path.exists(csv_path):
        log.warning(f"[{table}] CSV not found: {csv_path} — skipping")
        return 0

    df = pd.read_csv(csv_path, low_memory=False, encoding="utf-8-sig")

    # 2. Standardise columns + parse date
    df = standardise_columns(df, cfg["date_col"])

    # 3. Incremental filter: watermark < row_date <= batch_date
    date_col = cfg["date_col"]
    mask = (df[date_col] > watermark) & (df[date_col] <= batch_date)
    df   = df[mask].copy()

    if df.empty:
        log.info(f"[{table}] 0 new rows — nothing to ingest")
        return 0

    log.info(f"[{table}] {len(df):,} new rows found")

    # 4. Deduplicate on PK
    before = len(df)
    df.drop_duplicates(subset=[cfg["pk"]], keep="last", inplace=True)
    if len(df) < before:
        log.warning(f"[{table}] dropped {before - len(df)} duplicates on {cfg['pk']}")

    # 5. Add lineage metadata
    batch_id = str(uuid.uuid4())
    df["_ingestion_time"] = datetime.utcnow().isoformat()
    df["_source_system"]  = cfg["source_system"]
    df["_batch_id"]       = batch_id

    # 6. Group by date and upload one partition per distinct date
    df["_partition_date"] = df[date_col]
    total_uploaded = 0

    for part_date, part_df in df.groupby("_partition_date"):
        part_df = part_df.drop(columns=["_partition_date"])
        upload_parquet_to_minio(minio_client, table, part_df, part_date)
        total_uploaded += len(part_df)

    # 7. Advance watermark (only after all partitions uploaded)
    set_watermark(wm_key, batch_date)
    log.info(f"[{table}] watermark advanced to {batch_date}")

    return total_uploaded


# ─────────────────────────────────────────────────────────────
# ENTRY POINT
# ─────────────────────────────────────────────────────────────

def run_ingestion(batch_date: date | None = None) -> dict:
    """
    Run incremental ingestion for all tables.
    Returns summary dict {table: {"status": ..., "rows": ...}}.
    """
    if batch_date is None:
        batch_date = date.today() - timedelta(days=1)

    log.info("=" * 60)
    log.info(f"INCREMENTAL INGESTION  |  batch_date={batch_date}")
    log.info("=" * 60)

    minio_client = get_minio_client()
    summary: dict[str, dict] = {}

    for table, cfg in TABLE_CONFIG.items():
        try:
            n = ingest_table(table, cfg, minio_client, batch_date)
            summary[table] = {"status": "ok", "rows": n}
        except Exception as exc:
            log.error(f"[{table}] FAILED: {exc}", exc_info=True)
            summary[table] = {"status": "error", "error": str(exc)}

    log.info("\n" + "=" * 60)
    log.info("INGESTION SUMMARY")
    log.info("=" * 60)
    for tbl, res in summary.items():
        if res["status"] == "ok":
            log.info(f"  {tbl:<25} OK    ({res['rows']:>8,} rows)")
        else:
            log.error(f"  {tbl:<25} FAIL  {res['error']}")

    failed = [t for t, r in summary.items() if r["status"] == "error"]
    if failed:
        raise RuntimeError(f"Ingestion failed for tables: {failed}")

    return summary


if __name__ == "__main__":
    import sys
    override = None
    if len(sys.argv) == 2:
        override = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        log.info(f"CLI override: batch_date={override}")
    run_ingestion(override)
