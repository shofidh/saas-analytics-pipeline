"""
reset_watermarks.py
====================
Resets all pipeline watermarks to the pipeline start date (2024-01-01).
Run this on a fresh docker-compose up to ensure the full dataset is processed.
"""

import json
import os

WATERMARK_DIR = os.getenv("WATERMARK_DIR", os.path.join("data", "watermarks"))
PIPELINE_START = "2024-01-01"

WATERMARK_KEYS = [
    "ingestion_accounts",
    "ingestion_subscriptions",
    "ingestion_churn_events",
    "ingestion_feature_usage",
    "ingestion_support_tickets",
    "transform_accounts",
    "transform_subscriptions",
    "transform_churn_events",
    "transform_feature_usage",
    "transform_support_tickets",
    "warehouse_dim_accounts",
    "warehouse_fact_subscriptions",
    "warehouse_fact_churn_events",
    "warehouse_fact_feature_usage",
    "warehouse_fact_support_tickets",
]

os.makedirs(WATERMARK_DIR, exist_ok=True)
watermarks = {k: PIPELINE_START for k in WATERMARK_KEYS}
path = os.path.join(WATERMARK_DIR, "watermarks.json")

with open(path, "w") as f:
    json.dump(watermarks, f, indent=2)

print(f"All {len(WATERMARK_KEYS)} watermarks reset to {PIPELINE_START} → {path}")
