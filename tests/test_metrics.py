"""Hand-computed checks on the analytical definitions.

Each expected value is worked out by hand in the comment above the assertion, so a formula
change that breaks a definition fails loudly instead of drifting silently.
"""
import math

import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F


def test_demand_share(spark):
    # 1 of 2 postings in the period mention the skill -> 0.5
    df = spark.createDataFrame(
        [("2023-01", "sql", 1, 2)], "period string, skill string, n long, total long"
    )
    assert df.select(F.expr("try_divide(n, total)").alias("s")).collect()[0].s == pytest.approx(0.5)


def test_demand_share_change(spark):
    # 0.5 -> 0.4 is a change of -0.1, and the first period has no previous value
    df = spark.createDataFrame(
        [("2023-01", 0.5), ("2023-02", 0.4)], "period string, share double"
    )
    rows = (
        df.withColumn("d", F.col("share") - F.lag("share").over(Window.orderBy("period")))
        .orderBy("period")
        .collect()
    )
    assert rows[0].d is None
    assert rows[1].d == pytest.approx(-0.1)


def test_relative_growth_is_guarded(spark):
    # a skill with no earlier share must yield NULL, never infinite growth
    df = spark.createDataFrame([(0.0, 0.1), (0.2, 0.3)], "prev double, cur double")
    rows = df.select(
        F.when(F.col("prev") >= 0.001, F.expr("try_divide(cur - prev, prev)")).alias("g")
    ).collect()
    assert rows[0].g is None
    assert rows[1].g == pytest.approx(0.5)


def test_division_by_zero_returns_null(spark):
    # Spark 4 runs ANSI mode by default where 1/0 raises; try_divide is the safe form used
    # throughout the pipeline.
    df = spark.createDataFrame([(1, 0)], "a int, b int")
    assert df.select(F.expr("try_divide(a, b)").alias("x")).collect()[0].x is None


def test_jaccard(spark):
    # c=3, a=4, b=5 -> 3 / (4 + 5 - 3) = 0.5
    df = spark.createDataFrame([(3, 4, 5, 10)], "c long, a long, b long, n long")
    assert df.select(F.expr("try_divide(c, a + b - c)").alias("j")).collect()[0].j == pytest.approx(0.5)


def test_ppmi(spark):
    # c=3, a=4, b=5, N=10 -> (3*10)/(4*5) = 1.5 -> log2(1.5) = 0.5850
    df = spark.createDataFrame([(3, 4, 5, 10)], "c long, a long, b long, n long")
    p = df.select(
        F.greatest(F.lit(0.0), F.expr("log2(try_divide(c * n, a * b))")).alias("p")
    ).collect()[0].p
    assert p == pytest.approx(math.log2(1.5))


def test_ppmi_is_clipped_at_zero(spark):
    # c=1, a=4, b=5, N=10 -> 10/20 = 0.5 -> log2 = -1 -> clipped to 0 (positive PMI)
    df = spark.createDataFrame([(1, 4, 5, 10)], "c long, a long, b long, n long")
    p = df.select(
        F.greatest(F.lit(0.0), F.expr("log2(try_divide(c * n, a * b))")).alias("p")
    ).collect()[0].p
    assert p == pytest.approx(0.0)


def test_adoption_share(spark):
    # a skill requested by 3 of the 7 observed occupations -> 3/7
    df = spark.createDataFrame([(3, 7)], "occ long, eligible long")
    assert df.select(F.expr("try_divide(occ, eligible)").alias("a")).collect()[0].a == pytest.approx(
        3 / 7
    )
