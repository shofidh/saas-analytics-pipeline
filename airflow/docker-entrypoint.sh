#!/bin/bash
set -e

echo "Waiting for ClickHouse..."
until curl -sf http://clickhouse-saas:8123/ping > /dev/null 2>&1; do sleep 3; done
echo "ClickHouse ready."

echo "Waiting for MinIO..."
until curl -sf http://minio-saas:9000/minio/health/live > /dev/null 2>&1; do sleep 3; done
echo "MinIO ready."

echo "Initialising Airflow DB..."
airflow db init

echo "Creating admin user (idempotent)..."
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password password123 || true

echo "Starting Airflow standalone (webserver + scheduler)..."
exec airflow standalone