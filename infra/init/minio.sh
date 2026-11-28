#!/bin/sh
set -eu

attempt=1
until mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null 2>&1; do
    if [ "$attempt" -ge 60 ]; then
        echo 'MinIO did not accept its configured credentials.' >&2
        exit 1
    fi
    attempt=$((attempt + 1))
    sleep 2
done

for bucket in etl-raw etl-cleaned; do
    if ! mc mb --ignore-existing "local/$bucket" >/dev/null 2>&1; then
        echo "MinIO could not prepare bucket $bucket." >&2
        exit 1
    fi
done
echo 'MinIO buckets are ready.'
