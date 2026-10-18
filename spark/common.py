"""Shared Spark session and paths for the Phase 5 jobs."""
import os

from pyspark.sql import SparkSession

HDFS = os.environ.get("JOBMARKET_ROOT", "/jobmarket")
CLEAN_POSTINGS = f"{HDFS}/cleaned/postings"
CLEAN_SKILLS = f"{HDFS}/cleaned/job_skills"
WAREHOUSE = f"{HDFS}/warehouse"
HIVE_SKILL_DEMAND = "/user/hive/warehouse/jobmarket.db/skill_demand"


def get_spark(app):
    return (
        SparkSession.builder.appName(app)
        .master(os.environ.get("SPARK_MASTER", "local[*]"))
        # single node: the default 200 shuffle partitions is pure overhead
        .config("spark.sql.shuffle.partitions", "16")
        # Spark 4 defaults to ANSI: divisions throw instead of returning NULL.
        # Every division in these jobs goes through try_divide/try_cast.
        .config("spark.sql.ansi.enabled", "true")
        .getOrCreate()
    )


def write_parquet(df, name):
    path = f"{WAREHOUSE}/{name}"
    df.write.mode("overwrite").parquet(path)
    print(f"wrote {path}")


def write_csv(df, path):
    """Small result sets for the dashboard (one part file).

    The default filesystem is HDFS here, so a relative path would land in HDFS.
    Force an absolute local file:// URI.
    """
    local = path if path.startswith("file:") else "file://" + os.path.abspath(path)
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(local)
    print(f"wrote {local}")
