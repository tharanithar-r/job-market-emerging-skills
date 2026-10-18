#!/usr/bin/env bash
# Phase 3 — run the Pig ETL against HDFS. Idempotent: drops previous outputs first.
set -euo pipefail
cd "$(dirname "$0")/.."
PROJECT=$PWD
ROOT=${HDFS_ROOT:-/jobmarket}
TMP=$PROJECT/tmp
mkdir -p "$TMP"/{mapred,hadooptmp,staging}

# Verified requirements on this machine:
#  - piggybank.jar         -> CSVExcelStorage (quoted CSV)
#  - parquet-pig-bundle    -> ParquetStorer (not on the default Pig classpath)
#  - commons-collections 3 -> CircularFifoBuffer, needed by CSVExcelStorage
export PIG_CLASSPATH="/opt/pig/lib/piggybank.jar:/opt/pig/build/ivy/lib/Pig/parquet-pig-bundle-1.9.0.jar:/opt/pig/lib/hadoop3-runtime/commons-collections-3.2.2.jar"
export PIG_OPTS="-Dmapreduce.framework.name=${MR_FRAMEWORK:-local} -Djava.io.tmpdir=$TMP -Dpig.temp.dir=$TMP -Dhadoop.tmp.dir=$TMP/hadooptmp -Dmapred.local.dir=$TMP/mapred -Dmapreduce.cluster.local.dir=$TMP/mapred -Dmapreduce.jobtracker.staging.root.dir=$TMP/staging"
# Execution note: local MapReduce over HDFS by default. mapred-site.xml sets yarn, but the
# ResourceManager cannot start in a restricted shell on this box, and single-node YARN buys
# nothing. Once `start-yarn.sh` succeeds, run with: MR_FRAMEWORK=yarn ./scripts/run_pig.sh

hdfs dfs -rm -r -f "$ROOT/cleaned/postings" "$ROOT/cleaned/job_skills" "$ROOT/warehouse/pipeline_quality" >/dev/null
hdfs dfs -mkdir -p "$ROOT/cleaned" "$ROOT/warehouse/pipeline_quality"

echo "=== 3a clean_postings ==="
pig -f pig/clean_postings.pig

echo "=== 3b build_job_skills ==="
pig -f pig/build_job_skills.pig

echo "=== pipeline_quality ==="
hdfs dfs -cat "$ROOT/warehouse/pipeline_quality/*/*" 2>/dev/null || hdfs dfs -cat "$ROOT/warehouse/pipeline_quality"/*/*
