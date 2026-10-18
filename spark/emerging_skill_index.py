"""Emerging Skill Index: three percentile-ranked components, equal weights, plus validation.

Components (decision D7):
  G  demand-share change in the final period
  T  OLS slope of the skill's monthly demand share across all 12 periods
  C  change in mean PPMI of the skill's pairs, final period minus first

Occupational adoption was dropped from the index (decision D7-revised): with only 7
occupations almost every skill appears in all of them, adoption_share saturates at 1.0 and
the component carries no discriminating information. Adoption is still computed and
reported in its own table (`occupational_adoption`), just not scored.

Salary is also excluded (covers 2.8% of rows) and reported separately.

Every component is percentile-ranked across eligible skills, so the index compares ranks,
not incompatible units.
"""
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import get_spark, write_csv, write_parquet

WEIGHTS = {"G": 1 / 3, "T": 1 / 3, "C": 1 / 3}
MIN_SKILL_POSTINGS = 50

spark = get_spark("emerging_skill_index")

trend = spark.read.parquet("/jobmarket/warehouse/skill_trends")
pairs = spark.read.parquet("/jobmarket/warehouse/skill_pairs")

last = trend.agg(F.max("period")).first()[0]
first = trend.agg(F.min("period")).first()[0]
print(f"periods: {first} .. {last}")

# G - share change in the final period
g = trend.where(F.col("period") == last).select(
    "skill_norm", F.col("demand_share_change").alias("G_raw")
)

# T - OLS slope of demand share over the 12 periods
x = Window.orderBy("period")
slope = (
    trend.withColumn("x", F.dense_rank().over(x) - 1)
    .groupBy("skill_norm")
    .agg(
        F.count("*").alias("n"),
        F.sum("x").alias("sx"),
        F.sum("demand_share").alias("sy"),
        F.sum(F.col("x") * F.col("demand_share")).alias("sxy"),
        F.sum(F.col("x") * F.col("x")).alias("sxx"),
    )
    .withColumn("T_raw", F.expr("try_divide(n * sxy - sx * sy, n * sxx - sx * sx)"))
    .select("skill_norm", "T_raw")
)

# C - mean PPMI per skill, final minus first period (a skill appears as a and as b)
side = pairs.select("period", F.col("skill_a").alias("skill_norm"), "ppmi").union(
    pairs.select("period", F.col("skill_b").alias("skill_norm"), "ppmi")
)
mean_ppmi = side.groupBy("period", "skill_norm").agg(F.avg("ppmi").alias("m"))
c = (
    mean_ppmi.where(F.col("period") == last)
    .select("skill_norm", F.col("m").alias("last_m"))
    .join(
        mean_ppmi.where(F.col("period") == first).select(
            "skill_norm", F.col("m").alias("first_m")
        ),
        "skill_norm",
        "left",
    )
    .withColumn("C_raw", F.col("last_m") - F.coalesce(F.col("first_m"), F.lit(0.0)))
    .select("skill_norm", "C_raw")
)

eligible = (
    trend.groupBy("skill_norm")
    .agg(F.sum("posting_count").alias("total_posting_count"))
    .where(F.col("total_posting_count") >= MIN_SKILL_POSTINGS)
    .select("skill_norm")
)

base = (
    g.join(slope, "skill_norm", "left")
    .join(c, "skill_norm", "left")
    .join(eligible, "skill_norm")
)

# percentile rank each raw component (higher raw = higher score)
for raw, out in [("G_raw", "G"), ("T_raw", "T"), ("C_raw", "C")]:
    base = base.withColumn(out, F.percent_rank().over(Window.orderBy(raw)))

available = sum(
    [F.when(F.col(c_).isNotNull(), F.lit(1)).otherwise(F.lit(0)) for c_ in WEIGHTS]
)
base = base.withColumn("components_available", available)

# weighted sum over available components only, renormalised by available weight
esi = base.withColumn(
    "esi_score",
    F.expr(
        "try_divide({}, {})".format(
            " + ".join([f"{w} * coalesce({c}, 0.0)" for c, w in WEIGHTS.items()]),
            " + ".join(
                [f"(CASE WHEN {c} IS NULL THEN 0.0 ELSE {w} END)" for c, w in WEIGHTS.items()]
            ),
        )
    ),
)

esi = esi.withColumn("rank", F.row_number().over(Window.orderBy(F.desc("esi_score"))))
write_parquet(esi, "esi_scores")
write_csv(esi.orderBy("rank").limit(50), "outputs/spark/esi_top50.csv")

# ---- validation: compare against frequency-only and growth-only rankings ----
freq = trend.where(F.col("period") == last).select(
    "skill_norm", F.col("posting_count").alias("freq")
)
scored = esi.join(freq, "skill_norm", "left")


def top(sort_col, n=10):
    return [r.skill_norm for r in scored.orderBy(F.desc(sort_col)).limit(n).collect()]


esi_top = top("esi_score")
freq_top = top("freq")
growth_top = top("G_raw")

print("ESI top-10      :", esi_top)
print("frequency top-10:", freq_top)
print("growth top-10   :", growth_top)
print("overlap freq/esi  :", len(set(esi_top) & set(freq_top)), "/10")
print("overlap growth/esi:", len(set(esi_top) & set(growth_top)), "/10")

baselines = spark.createDataFrame(
    [
        ("esi", ",".join(esi_top)),
        ("frequency_only", ",".join(freq_top)),
        ("growth_only", ",".join(growth_top)),
    ],
    "ranking string, top10 string",
)
write_csv(baselines, "outputs/spark/esi_baselines.csv")

# ---- rank correlation ------------------------------------------------------
# Spearman = Pearson correlation of ranks. `rank()` (not row_number) gives tied skills
# the same rank, so the coefficient is not distorted by arbitrary tie ordering.
ranked = (
    scored.withColumn("rank_esi", F.rank().over(Window.orderBy(F.desc("esi_score"))))
    .withColumn("rank_freq", F.rank().over(Window.orderBy(F.desc("freq"))))
    .withColumn("rank_growth", F.rank().over(Window.orderBy(F.desc("G_raw"))))
)
correlation = spark.createDataFrame(
    [
        (name, float(ranked.stat.corr(a, b)))
        for name, a, b in [
            ("esi_vs_frequency", "rank_esi", "rank_freq"),
            ("esi_vs_growth", "rank_esi", "rank_growth"),
            ("frequency_vs_growth", "rank_freq", "rank_growth"),
        ]
    ],
    "comparison string, spearman double",
)
write_csv(correlation, "outputs/spark/esi_rank_correlation.csv")
print(f"--- Spearman rank correlation over {scored.count()} scored skills ---")
correlation.show(truncate=False)

# ---- sensitivity: does the top-10 survive different weights? ----
WEIGHT_SETS = {
    "equal": {"G": 1 / 3, "T": 1 / 3, "C": 1 / 3},
    "growth_heavy": {"G": 0.50, "T": 0.30, "C": 0.20},
    "trend_heavy": {"G": 0.30, "T": 0.50, "C": 0.20},
    "cooccurrence_heavy": {"G": 0.20, "T": 0.20, "C": 0.60},
}
rows = []
for name, w in WEIGHT_SETS.items():
    expr = " + ".join([f"{w[c]} * coalesce({c}, 0.0)" for c in w])
    d = esi.withColumn("s", F.expr(expr))
    t = [r.skill_norm for r in d.orderBy(F.desc("s")).limit(10).collect()]
    rows.append((name, ",".join(t), float(len(set(t) & set(esi_top))) / 10.0))

sens = spark.createDataFrame(
    rows, "weight_set string, top10 string, overlap_with_equal double"
)
write_csv(sens, "outputs/spark/esi_sensitivity.csv")
print("--- sensitivity ---")
sens.show(truncate=False)

print("--- ESI top 15 ---")
esi.orderBy("rank").select(
    "skill_norm", "G", "T", "C", "components_available", "esi_score", "rank"
).show(15, truncate=False)

spark.stop()
