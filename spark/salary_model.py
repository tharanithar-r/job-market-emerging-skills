"""Spark MLlib salary model: time-based split, baseline -> linear -> gradient-boosted trees.

Target is advertised annual salary, which only 2.8% of postings disclose, so this models
salary-disclosing postings. The split is by posting date (train Jan-Sep, test Oct-Dec),
not random, so the evaluation matches the "predict on later postings" question.
"""
from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.ml.regression import GBTRegressor, LinearRegression
from pyspark.sql import Window
from pyspark.sql import functions as F

from common import CLEAN_POSTINGS, CLEAN_SKILLS, get_spark, write_csv

TRAIN_END = "2023-09-30"
TOP_SKILLS = 10

spark = get_spark("salary_model")

postings = spark.read.parquet(CLEAN_POSTINGS)

salary = (
    postings.withColumn("salary", F.expr("try_cast(salary_year_avg as double)"))
    .where(F.col("salary_rate") == "year")
    .where(F.col("salary").isNotNull() & (F.col("salary") >= 15000) & (F.col("salary") <= 960000))
    .where(F.col("posted_date").isNotNull())
    .select(
        "job_key", "posted_date", "salary", "occupation", "seniority", "job_country",
        "work_from_home", "no_degree_mention", "health_insurance", "job_schedule_type",
    )
)

# top-N skills as binary indicators (bounded pivot; the salary subset is small)
skills = spark.read.parquet(CLEAN_SKILLS)
top = [
    r.skill_norm
    for r in skills.groupBy("skill_norm")
    .agg(F.count("*").alias("n"))
    .orderBy(F.desc("n"))
    .limit(TOP_SKILLS)
    .collect()
    if r.skill_norm and r.skill_norm.strip()  # empty names would break VectorAssembler
]
print("skill indicator columns:", top)
indicators = (
    skills.where(F.col("skill_norm").isin(top))
    .groupBy("job_key")
    .pivot("skill_norm", top)
    .agg(F.lit(1.0))
    .na.fill(0.0)
)

df = salary.join(indicators, "job_key", "left").na.fill(0.0, subset=top)

CATS = ["occupation", "seniority", "job_country", "job_schedule_type"]
FLAGS = ["work_from_home", "no_degree_mention", "health_insurance"]

# 16 postings have an empty job_schedule_type. OneHotEncoder names an attribute after each
# category, and an empty name raises "Cannot have an empty string for name" — so label them.
df = df.na.replace("", "unknown", subset=CATS)

# work_from_home / no_degree_mention / health_insurance arrive as 'true'/'false' strings
# from Pig; VectorAssembler only accepts numeric columns.
for flag in FLAGS:
    df = df.withColumn(flag, F.when(F.col(flag) == "true", 1.0).otherwise(0.0))

df.cache()
print(f"salary rows: {df.count():,}")

train = df.where(F.col("posted_date") <= TRAIN_END)
test = df.where(F.col("posted_date") > TRAIN_END)
print(f"train: {train.count():,}  test: {test.count():,}")

indexers = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in CATS]
encoders = [OneHotEncoder(inputCol=f"{c}_idx", outputCol=f"{c}_oh", dropLast=True) for c in CATS]
assembler = VectorAssembler(
    inputCols=[f"{c}_oh" for c in CATS] + FLAGS + top, outputCol="features"
)

evaluator = RegressionEvaluator(labelCol="salary", predictionCol="prediction")

def score(name, predictions):
    out = []
    for metric in ("rmse", "mae", "r2"):
        evaluator.setMetricName(metric)
        out.append(evaluator.evaluate(predictions))
    print(f"{name:18} RMSE={out[0]:,.0f}  MAE={out[1]:,.0f}  R2={out[2]:.4f}")
    return (name, out[0], out[1], out[2])

results = []

# baseline: predict the training mean
mean_salary = train.agg(F.avg("salary")).first()[0]
baseline = test.withColumn("prediction", F.lit(mean_salary))
results.append(score("mean_baseline", baseline))

for name, estimator in [
    ("linear_regression", LinearRegression(labelCol="salary", maxIter=50, regParam=0.1)),
    ("gbt_regressor", GBTRegressor(labelCol="salary", maxIter=40, maxDepth=4, seed=42)),
]:
    pipeline = Pipeline(stages=indexers + encoders + [assembler, estimator])
    model = pipeline.fit(train)
    results.append(score(name, model.transform(test)))

metrics = spark.createDataFrame(results, "model string, rmse double, mae double, r2 double")
write_csv(metrics, "outputs/spark/salary_metrics.csv")
metrics.show(truncate=False)

# ---- descriptive salary association (dashboard exports) ----------------------
by_occupation = (
    df.groupBy("occupation")
    .agg(
        F.count("*").alias("n"),
        F.expr("percentile_approx(salary, 0.5)").alias("median_salary"),
        F.expr("percentile_approx(salary, 0.25)").alias("p25"),
        F.expr("percentile_approx(salary, 0.75)").alias("p75"),
    )
    .where(F.col("n") >= 100)
    .orderBy(F.desc("median_salary"))
)
write_csv(by_occupation, "outputs/spark/salary_by_occupation.csv")
by_occupation.show(truncate=False)

# salary association per skill: descriptive only, never causal
by_skill = (
    skills.join(df.select("job_key", "salary"), "job_key")
    .groupBy("skill_norm")
    .agg(
        F.count("*").alias("n"),
        F.expr("percentile_approx(salary, 0.5)").alias("median_salary"),
    )
    .where(F.col("n") >= 100)
    .orderBy(F.desc("median_salary"))
)
write_csv(by_skill, "outputs/spark/salary_by_skill.csv")
by_skill.show(10, truncate=False)

spark.stop()
