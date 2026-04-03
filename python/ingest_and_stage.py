"""
ingest_and_stage.py
====================
DEPRECATED: Use incremental_ingestion.py + transform_incremental.py in production.
This wrapper exists for local/dev convenience only — it runs both steps sequentially.

Flow:
  1. incremental_ingestion.py  →  CSV → MinIO (partitioned Parquet)
  2. transform_incremental.py  →  MinIO → ClickHouse staging

Usage:
  python ingest_and_stage.py [YYYY-MM-DD]
"""

from __future__ import annotations

import logging
import sys
from datetime import date, datetime, timedelta

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def run_ingest_and_stage(batch_date: date | None = None) -> None:
    """
    Convenience wrapper: runs ingestion (CSV → MinIO) then
    transformation (MinIO → staging) in sequence.
    """
    if batch_date is None:
        batch_date = date.today() - timedelta(days=1)

    log.warning(
        "╔══════════════════════════════════════════════════════════╗\n"
        "║  DEPRECATED — Use the two-step pipeline in production:  ║\n"
        "║    1. python incremental_ingestion.py [date]            ║\n"
        "║    2. python transform_incremental.py [date]            ║\n"
        "║  This wrapper exists for local/dev convenience only.    ║\n"
        "╚══════════════════════════════════════════════════════════╝"
    )

    # Step 1: Ingest raw CSVs → MinIO
    log.info("=" * 60)
    log.info("STEP 1/2: INCREMENTAL INGESTION (CSV → MinIO)")
    log.info("=" * 60)
    from incremental_ingestion import run_ingestion
    run_ingestion(batch_date)

    # Step 2: Transform MinIO → ClickHouse staging
    log.info("=" * 60)
    log.info("STEP 2/2: INCREMENTAL TRANSFORM (MinIO → staging)")
    log.info("=" * 60)
    from transform_incremental import run_transform
    run_transform(batch_date)

    log.info("\n" + "=" * 60)
    log.info("INGEST & STAGE COMPLETE")
    log.info("=" * 60)


if __name__ == "__main__":
    override = None
    if len(sys.argv) == 2:
        override = datetime.strptime(sys.argv[1], "%Y-%m-%d").date()
        log.info(f"CLI override: batch_date={override}")
    run_ingest_and_stage(override)
