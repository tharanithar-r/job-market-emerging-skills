"""Occupational breadth: how many distinct occupations request each skill per period."""
from pyspark.sql import functions as F

from common import CLEAN_POSTINGS, CLEAN_SKILLS, get_spark, write_csv, write_parquet

spark = get_spark("occupational_adoption")

per_skill = (
    spark.read.parquet(CLEAN_SKILLS)
    .groupBy("period", "skill_norm")
    .agg(F.countDistinct("occupation").alias("occupation_count"))
)

# denominator: occupations actually observed in that period, so coverage change is visible
eligible = (
    spark.read.parquet(CLEAN_POSTINGS)
    .groupBy("period")
    .agg(F.countDistinct("occupation").alias("eligible_occupations"))
)

adoption = per_skill.join(eligible, "period", "left").withColumn(
    "adoption_share", F.expr("try_divide(occupation_count, eligible_occupations)")
)

write_parquet(adoption, "occupational_adoption")

last = adoption.agg(F.max("period")).first()[0]
write_csv(
    adoption.where(F.col("period") == last).orderBy(F.desc("adoption_share")).limit(20),
    "outputs/spark/adoption_top.csv",
)

print("--- widest occupational adoption in the final period ---")
adoption.where(F.col("period") == last).orderBy(F.desc("adoption_share")).select(
    "skill_norm", "occupation_count", "eligible_occupations", "adoption_share"
).show(10, truncate=False)

spark.stop()
