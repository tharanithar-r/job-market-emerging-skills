#!/usr/bin/env bash
# Phase 5 — run the Spark analytics jobs against HDFS.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p tmp outputs/spark

export HADOOP_CONF_DIR=/opt/hadoop-3.4.3/etc/hadoop
# MLlib imports numpy; the system python3.13 does not have it. Create with:
#   uv venv --python 3.13 .venv && uv pip install --python .venv/bin/python -r requirements.txt
export PYSPARK_PYTHON=${PYSPARK_PYTHON:-$PWD/.venv/bin/python}
export PYSPARK_DRIVER_PYTHON=${PYSPARK_DRIVER_PYTHON:-$PWD/.venv/bin/python}

# Spark 4 defaults to ANSI mode; the jobs use try_divide/try_cast wherever division can
# hit zero or a bad cast. Kept explicit here so the behaviour is visible.
CONF=(
  --master "${SPARK_MASTER:-local[*]}"
  --driver-memory 4g
  --conf spark.sql.shuffle.partitions=16
  --conf spark.sql.ansi.enabled=true
  --conf spark.driver.extraJavaOptions=-Djava.io.tmpdir="$PWD/tmp"
)

for job in skill_trends occupational_adoption skill_cooccurrence emerging_skill_index salary_model; do
  echo "=== $job ==="
  spark-submit "${CONF[@]}" "spark/$job.py"
done
