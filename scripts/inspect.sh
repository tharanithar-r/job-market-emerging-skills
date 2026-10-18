#!/usr/bin/env bash
# Print a read-only tour of everything the pipeline produced.
set -euo pipefail
cd "$(dirname "$0")/.."

hr() { echo; echo "=== $1 ==="; }

hr "1. DATASET (local)"
ls -lh data/raw/data_jobs.csv | awk '{print "   file:", $5, $9}'
python3 scripts/validate_dataset.py | sed -n '1,16p'

hr "2. HDFS LAYOUT"
hdfs dfs -du -h /jobmarket 2>/dev/null | grep -v WARN

hr "3. PIPELINE ACCOUNTING (Pig's own counters)"
hdfs dfs -cat /jobmarket/warehouse/pipeline_quality/clean_postings/part* 2>/dev/null | sort
hdfs dfs -cat /jobmarket/warehouse/pipeline_quality/build_job_skills/part* 2>/dev/null | sort

hr "4. WAREHOUSE TABLES (Parquet in HDFS)"
for t in skill_trends skill_pairs occupational_adoption esi_scores; do
  n=$(hdfs dfs -ls "/jobmarket/warehouse/$t" 2>/dev/null | grep -c 'parquet' || true)
  s=$(hdfs dfs -du -s -h "/jobmarket/warehouse/$t" 2>/dev/null | awk '{print $1$2}')
  printf '   %-24s %2s part file(s)  %s\n' "$t" "$n" "$s"
done

hr "5. DASHBOARD EXPORTS (outputs/spark)"
ls -1 outputs/spark | sed 's/^/   /'

hr "6. HIVE TABLES"
if command -v beeline >/dev/null; then
  TERM=dumb beeline -u "jdbc:hive2://" --silent=true \
    -e "USE jobmarket; SHOW TABLES;" 2>/dev/null | grep -E '^\|' || echo "   (beeline not reachable)"
else
  echo "   beeline not on PATH"
fi

echo
echo "Next: .venv/bin/streamlit run dashboard/app.py"
