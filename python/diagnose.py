"""Quick diagnostic: inspect CSV schema vs ClickHouse staging schema."""
import pandas as pd
import clickhouse_connect
import traceback

print("=" * 60)
print("SCHEMA DIAGNOSTIC")
print("=" * 60)

TABLES = {
    "accounts":       ("data/raw_data/accounts.csv",       "signup_date",  "staging.stg_accounts"),
    "subscriptions":  ("data/raw_data/subscriptions.csv",  "start_date",   "staging.stg_subscriptions"),
    "churn_events":   ("data/raw_data/churn_events.csv",   "churn_date",   "staging.stg_churn_events"),
    "feature_usage":  ("data/raw_data/feature_usage.csv",  "usage_date",   "staging.stg_feature_usage"),
    "support_tickets":("data/raw_data/support_tickets.csv","submitted_at", "staging.stg_support_tickets"),
}

ch = clickhouse_connect.get_client(
    host="clickhouse-saas", port=8123, username="default", password="password123"
)

for name, (csv_path, date_col, staging) in TABLES.items():
    print(f"\n── {name} ──")
    try:
        df = pd.read_csv(csv_path, low_memory=False, encoding="utf-8-sig", nrows=5)
        print(f"  CSV columns ({len(df.columns)}): {list(df.columns)}")

        ch_cols = ch.query(f"DESCRIBE {staging}").result_rows
        ch_col_names = [r[0] for r in ch_cols]
        print(f"  CH  columns ({len(ch_col_names)}): {ch_col_names}")

        missing_in_ch = [c for c in df.columns if c not in ch_col_names]
        missing_in_df = [c for c in ch_col_names if c not in df.columns
                         and not c.startswith("_")]
        if missing_in_ch:
            print(f"  ⚠ In CSV but not in ClickHouse staging: {missing_in_ch}")
        if missing_in_df:
            print(f"  ⚠ In ClickHouse but not in CSV: {missing_in_df}")

    except Exception as e:
        print(f"  ERROR: {e}")
        traceback.print_exc()

print("\nDone.")
