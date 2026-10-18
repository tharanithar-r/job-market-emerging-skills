"""Temporal demand features per (period, skill): share change, relative growth, breadth.

Base demand counts come from the Hive-built `skill_demand` table, so this stage genuinely
consumes Hive output rather than recomputing it.
"""
import sys

from pyspark.sql import Window
from pyspark.sql import functions as F

from common import CLEAN_SKILLS, HDFS, HIVE_SKILL_DEMAND, get_spark, write_csv, write_parquet

spark = get_spark("skill_trends")

demand = spark.read.parquet(HIVE_SKILL_DEMAND).withColumn(
    "demand_share", F.col("demand_share").cast("double")
)

# previous period's share, per skill
w = Window.partitionBy("skill_norm").orderBy("period")
trend = demand.withColumn("prev_demand_share", F.lag("demand_share").over(w))

trend = trend.withColumn(
    "demand_share_change", F.col("demand_share") - F.col("prev_demand_share")
).withColumn(
    # relative growth only where the prior share clears a floor: no infinite growth
    "relative_growth",
    F.when(
        F.col("prev_demand_share") >= F.lit(0.001),
        F.expr("try_divide(demand_share - prev_demand_share, prev_demand_share)"),
    ),
)

occupations = (
    spark.read.parquet(CLEAN_SKILLS)
    .groupBy("period", "skill_norm")
    .agg(F.countDistinct("occupation").alias("occupation_count"))
)

trend = trend.join(occupations, ["period", "skill_norm"], "left")
write_parquet(trend, "skill_trends")

last = trend.agg(F.max("period")).first()[0]
print(f"last period: {last}")
write_csv(
    trend.where(F.col("period") == last)
    .orderBy(F.desc("demand_share_change"))
    .limit(20),
    "outputs/spark/top_movers.csv",
)

print("--- biggest share gainers in the final period ---")
trend.where(F.col("period") == last).orderBy(F.desc("demand_share_change")).select(
    "skill_norm", "posting_count", "demand_share", "demand_share_change", "occupation_count"
).show(10, truncate=False)

# ---- dashboard exports -------------------------------------------------------
eligible = (
    trend.groupBy("skill_norm")
    .agg(F.sum("posting_count").alias("total"))
    .where(F.col("total") >= 50)
    .select("skill_norm")
)
write_csv(trend.join(eligible, "skill_norm"), "outputs/spark/skill_trends_all.csv")

quality = (
    spark.read.option("sep", "\t")
    .schema("stage string, records long")
    .csv(f"{HDFS}/warehouse/pipeline_quality/clean_postings")
    .union(
        spark.read.option("sep", "\t")
        .schema("stage string, records long")
        .csv(f"{HDFS}/warehouse/pipeline_quality/build_job_skills")
    )
)
write_csv(quality, "outputs/spark/pipeline_quality.csv")

spark.stop()