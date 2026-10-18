#!/usr/bin/env bash
# Phase 2 — upload the raw dataset to HDFS and verify. Idempotent.
set -euo pipefail
cd "$(dirname "$0")/.."

ROOT=${HDFS_ROOT:-/jobmarket}
SRC=data/raw/data_jobs.csv

[[ -f "$SRC" ]] || { echo "missing $SRC — run scripts/download_data.sh first"; exit 1; }

hdfs dfs -mkdir -p "$ROOT/raw/postings"
hdfs dfs -put -f "$SRC" "$ROOT/raw/postings/"

local_rows=$(($(wc -l < "$SRC") - 1))
hdfs_rows=$(hdfs dfs -cat "$ROOT/raw/postings/data_jobs.csv" | wc -l | tr -d ' ')

echo "local rows (excl header): $local_rows"
echo "hdfs  rows (incl header): $hdfs_rows"
[[ "$((hdfs_rows - 1))" -eq "$local_rows" ]] || { echo "ROW MISMATCH"; exit 1; }
echo "OK — reconciled"
hdfs dfs -ls -h "$ROOT/raw/postings/"
