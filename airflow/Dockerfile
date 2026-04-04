FROM apache/airflow:2.8.1-python3.11

USER root
RUN apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir pandas clickhouse-connect minio pyarrow dbt-core dbt-clickhouse