"""
watermark_tracker.py
====================
Manages per-table watermarks (last successfully processed date) for incremental
pipeline execution. State is persisted in a JSON file so it survives container
restarts while remaining portable.

Schema of watermarks.json:
{
    "accounts":          "2024-01-28",
    "subscriptions":     "2024-01-28",
    "churn_events":      "2024-01-28",
    "feature_usage":     "2024-01-28",
    "support_tickets":   "2024-01-28"
}
"""

import json
import os
from datetime import datetime, date

# ===========================================================
# CONFIGURATION
# ===========================================================
WATERMARK_DIR  = os.getenv("WATERMARK_DIR",  "/app/data/watermarks")
WATERMARK_FILE = os.path.join(WATERMARK_DIR, "watermarks.json")

# Pipeline start date — used when no watermark exists yet
PIPELINE_START_DATE = date(2024, 1, 1)


# ===========================================================
# HELPERS
# ===========================================================

def _load_state() -> dict:
    """Load the full watermark state from disk. Returns {} if file missing."""
    if not os.path.exists(WATERMARK_FILE):
        return {}
    with open(WATERMARK_FILE, "r") as f:
        return json.load(f)


def _save_state(state: dict) -> None:
    """Persist the watermark state to disk atomically."""
    os.makedirs(WATERMARK_DIR, exist_ok=True)
    tmp_path = WATERMARK_FILE + ".tmp"
    with open(tmp_path, "w") as f:
        json.dump(state, f, indent=2, default=str)
    os.replace(tmp_path, WATERMARK_FILE)   # atomic swap


# ===========================================================
# PUBLIC API
# ===========================================================

def get_watermark(table: str) -> date:
    """
    Return the last successfully processed date for *table*.

    If no watermark exists for the table, returns PIPELINE_START_DATE - 1 day
    so the first run ingests data starting from PIPELINE_START_DATE.
    """
    state = _load_state()
    raw   = state.get(table)
    if raw is None:
        from datetime import timedelta
        return PIPELINE_START_DATE - _one_day()
    if isinstance(raw, date) and not isinstance(raw, datetime):
        return raw
    if isinstance(raw, datetime):
        return raw.date()
    return datetime.strptime(raw, "%Y-%m-%d").date()


def set_watermark(table: str, processed_date: date) -> None:
    """
    Update the watermark for *table* to *processed_date*.

    Only advances (never rolls back) to prevent accidental data loss.
    """
    state   = _load_state()
    current = state.get(table)

    if current is not None:
        current_date = datetime.strptime(current, "%Y-%m-%d").date() \
                       if isinstance(current, str) else current
        if isinstance(processed_date, datetime):
            processed_date = processed_date.date()
        if processed_date <= current_date:
            print(f"[watermark] {table}: skipping update — "
                  f"new={processed_date} <= current={current_date}")
            return

    state[table] = str(processed_date)
    _save_state(state)
    print(f"[watermark] {table}: updated to {processed_date}")


def get_all_watermarks() -> dict:
    """Return all watermarks as a dict of {table: date_str}."""
    return _load_state()


def reset_watermark(table: str) -> None:
    """Reset a single table's watermark (use for backfill / re-processing)."""
    state = _load_state()
    if table in state:
        del state[table]
        _save_state(state)
        print(f"[watermark] {table}: reset to pipeline start")


def _one_day():
    """Helper to avoid circular import."""
    from datetime import timedelta
    return timedelta(days=1)


# ===========================================================
# CLI helper — print current watermarks
# ===========================================================
if __name__ == "__main__":
    wm = get_all_watermarks()
    if not wm:
        print("No watermarks set yet.")
    else:
        print("Current watermarks:")
        for tbl, dt in wm.items():
            print(f"  {tbl:<25} {dt}")
