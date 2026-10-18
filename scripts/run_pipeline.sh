#!/usr/bin/env bash
# End-to-end pipeline: download -> HDFS -> Pig -> Hive -> Spark -> dashboard outputs.
# Each stage is idempotent, so this can be re-run from scratch.
set -euo pipefail
cd "$(dirname "$0")/.."

if [[ ! -d .venv ]]; then
  echo "== creating venv =="
  uv venv --python 3.13 .venv
  uv pip install --python .venv/bin/python -r requirements.txt
fi

echo "== 1/6 dataset =="
./scripts/download_data.sh

echo "== 2/6 validate =="
python3 scripts/validate_dataset.py

echo "== 3/6 HDFS ingest =="
./scripts/upload_to_hdfs.sh

echo "== 4/6 Pig ETL =="
./scripts/run_pig.sh

echo "== 5/6 Hive warehouse =="
./scripts/run_hive.sh

echo "== 6/6 Spark analytics + ESI + MLlib =="
./scripts/run_spark.sh

cat <<'EOT'

Pipeline complete.
  Dashboard : .venv/bin/streamlit run dashboard/app.py
  Tests     : .venv/bin/python -m pytest tests -q
  Outputs   : outputs/spark/       (warehouse tables are in HDFS /jobmarket/warehouse)
EOT
