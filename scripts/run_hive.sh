#!/usr/bin/env bash
# Phase 4 — build the Hive warehouse over the Parquet Pig produced.
#
# Three environment quirks on this box, all verified:
#  * TERM=dumb          Beeline's jline provider needs Java preview features and dies without a TTY.
#  * -e, not -f         `beeline -f file` echoes the statements here without executing them; -e runs them.
#  * mapreduce...=local Hive would otherwise target YARN, whose RM cannot start in this shell.
# Beeline also hangs ~2.5 min in JVM shutdown hooks, so every statement runs in ONE session.
set -euo pipefail
cd "$(dirname "$0")/.."
mkdir -p tmp/hive outputs/hive

export TERM=dumb
# The Hadoop wrapper defaults the client JVM to 256 MB, which the CTAS blows past.
export HADOOP_HEAPSIZE_MAX=4096
export HADOOP_HEAPSIZE=4096
export HADOOP_CLIENT_OPTS="-Xmx4g -Djava.io.tmpdir=$PWD/tmp/hive"

# Hive ignores JVM-level -D for job config, so every session setting is prepended as SET.
# Tez is not installed -> MR. Local mode + explicit scratch dirs, because the defaults live
# under /private/tmp which is not writable from this shell.
SETTINGS="SET hive.execution.engine=mr;
SET hive.aux.jars.path=;
SET mapreduce.framework.name=local;
SET mapreduce.input.fileinputformat.input.dir.recursive=true;
SET hadoop.tmp.dir=$PWD/tmp/hive;
SET mapred.local.dir=$PWD/tmp/mapred;
SET mapreduce.cluster.local.dir=$PWD/tmp/mapred;
SET mapreduce.jobtracker.staging.root.dir=$PWD/tmp/staging;"

SQL="$SETTINGS $(cat hive/create_tables.sql hive/validation_queries.sql hive/analytical_queries.sql \
      | grep -v '^--' | tr '\n' ' ')"

# A stuck Beeline JVM keeps the embedded Derby metastore locked; clear any before starting.
pkill -9 -f 'org.apache.hive.beeline.BeeLine' 2>/dev/null || true
sleep 1

# Hadoop's LocalDistributedCacheManager symlinks a literal "libjars/*" glob; if the directory
# does not exist the job dies with "ln: .../libjars/*: No such file or directory".
mkdir -p libjars && touch libjars/.keep

beeline -u "jdbc:hive2://" -e "$SQL" > outputs/hive/hive_run.txt 2>&1 || true

pkill -9 -f 'org.apache.hive.beeline.BeeLine' 2>/dev/null || true

grep -viE 'NativeCodeLoader|FunctionRegistry|DataNucleus|SLF4J|WARN|^\s+at |TimeoutException|shutdown' \
    outputs/hive/hive_run.txt | grep -E '\||No rows|Error|FAILED' || true
