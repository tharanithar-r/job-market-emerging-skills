"""Skill co-occurrence: pair counts plus Jaccard and PPMI.

Jaccard answers "how much do these skill sets overlap"; PPMI answers "is this pair more
common than chance". They disagree on purpose: Jaccard is inflated by ubiquitous skills,
PPMI punishes rare ones, so both are reported.
"""
from pyspark.sql import functions as F

from common import CLEAN_POSTINGS, CLEAN_SKILLS, get_spark, write_csv, write_parquet

MIN_SKILL_POSTINGS = 50
MIN_PAIR_SUPPORT = 50

spark = get_spark("skill_cooccurrence")

skills = spark.read.parquet(CLEAN_SKILLS)

# keep only skills with enough support to make a pair meaningful
supported = (
    skills.groupBy("skill_norm")
    .agg(F.count("*").alias("n"))
    .where(F.col("n") >= MIN_SKILL_POSTINGS)
    .select("skill_norm")
)
skills = skills.join(supported, "skill_norm")

a = skills.select("job_key", "period", F.col("skill_norm").alias("skill_a"))
b = skills.select("job_key", F.col("skill_norm").alias("skill_b"))
pairs = a.join(b, "job_key").where(F.col("skill_a") < F.col("skill_b"))

co = pairs.groupBy("period", "skill_a", "skill_b").agg(
    F.count("*").alias("cooccurrence_count")
)

per_skill = skills.groupBy("period", "skill_norm").agg(F.count("*").alias("n"))
co = co.join(
    per_skill.select(
        "period", F.col("skill_norm").alias("skill_a"), F.col("n").alias("skill_a_count")
    ),
    ["period", "skill_a"],
).join(
    per_skill.select(
        "period", F.col("skill_norm").alias("skill_b"), F.col("n").alias("skill_b_count")
    ),
    ["period", "skill_b"],
)

totals = spark.read.parquet(CLEAN_POSTINGS).groupBy("period").agg(
    F.count("*").alias("total_postings")
)
co = co.join(totals, "period")

co = co.withColumn(
    "jaccard",
    F.expr("try_divide(cooccurrence_count, skill_a_count + skill_b_count - cooccurrence_count)"),
).withColumn(
    # positive PMI: 0 means "no more common than chance"
    "ppmi",
    F.greatest(
        F.lit(0.0),
        F.expr(
            "log2(try_divide(cooccurrence_count * total_postings, skill_a_count * skill_b_count))"
        ),
    ),
)

co = co.where(F.col("cooccurrence_count") >= MIN_PAIR_SUPPORT)
write_parquet(co, "skill_pairs")

last = co.agg(F.max("period")).first()[0]
write_csv(
    co.where(F.col("period") == last)
    .orderBy(F.desc("cooccurrence_count"))
    .limit(50),
    "outputs/spark/top_pairs.csv",
)

print("--- strongest pairs by PPMI in the final period (support >= 50) ---")
co.where(F.col("period") == last).orderBy(F.desc("ppmi")).select(
    "skill_a", "skill_b", "cooccurrence_count", "jaccard", "ppmi"
).show(10, truncate=False)

spark.stop()
