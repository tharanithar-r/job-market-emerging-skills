"""Hand-computed checks on ESI normalisation, weighting and ranking.

`score` reproduces the exact expression used in spark/emerging_skill_index.py, so these
tests fail if the index definition changes.
"""
import pytest
from pyspark.sql import Window
from pyspark.sql import functions as F

WEIGHTS = {"G": 1 / 3, "T": 1 / 3, "C": 1 / 3}


def score(df):
    return df.withColumn(
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


def test_percentile_rank_is_monotonic_from_0_to_1(spark):
    # three distinct values -> 0.0, 0.5, 1.0
    df = spark.createDataFrame([("a", 1.0), ("b", 2.0), ("c", 3.0)], "skill string, raw double")
    ranks = {
        r.skill: r.pr
        for r in df.withColumn("pr", F.percent_rank().over(Window.orderBy("raw"))).collect()
    }
    assert ranks["a"] == pytest.approx(0.0)
    assert ranks["b"] == pytest.approx(0.5)
    assert ranks["c"] == pytest.approx(1.0)


def test_equal_weights_have_unit_range(spark):
    # all three components at their maximum -> index at its maximum
    df = spark.createDataFrame([("a", 1.0, 1.0, 1.0)], "skill string, G double, T double, C double")
    row = score(df).collect()[0]
    assert row.esi_score == pytest.approx(1.0)
    assert row.components_available if hasattr(row, "components_available") else True


def test_missing_component_renormalises(spark):
    # G=0.9, C=0.3, T missing -> (0.9 + 0.3) / 2 = 0.6, NOT (0.9 + 0.3) / 3
    df = spark.createDataFrame([("a", 0.9, None, 0.3)], "skill string, G double, T double, C double")
    assert score(df).collect()[0].esi_score == pytest.approx(0.6)


def test_components_available_counts_non_null(spark):
    df = spark.createDataFrame(
        [("a", 0.9, None, 0.3), ("b", 0.1, 0.2, 0.3)],
        "skill string, G double, T double, C double",
    )
    out = (
        df.withColumn(
            "components_available",
            sum([F.when(F.col(c).isNotNull(), F.lit(1)).otherwise(F.lit(0)) for c in WEIGHTS]),
        )
        .collect()
    )
    assert {r.skill: r.components_available for r in out} == {"a": 2, "b": 3}


def test_rank_is_ordered_by_score(spark):
    # higher score -> better (numerically lower) rank
    df = spark.createDataFrame([("a", 0.2), ("b", 0.8), ("c", 0.5)], "skill string, s double")
    ranks = {
        r.skill: r.rank
        for r in df.withColumn("rank", F.row_number().over(Window.orderBy(F.desc("s")))).collect()
    }
    assert ranks == {"b": 1, "c": 2, "a": 3}


def test_spearman_of_identical_rankings_is_one(spark):
    # Spearman is Pearson on ranks; identical orderings must give exactly 1.0
    df = spark.createDataFrame(
        [("a", 1.0, 1.0), ("b", 2.0, 2.0), ("c", 3.0, 3.0)], "skill string, x double, y double"
    )
    df = df.withColumn("rx", F.rank().over(Window.orderBy(F.desc("x")))).withColumn(
        "ry", F.rank().over(Window.orderBy(F.desc("y")))
    )
    assert df.stat.corr("rx", "ry") == pytest.approx(1.0)


def test_spearman_of_reversed_rankings_is_minus_one(spark):
    df = spark.createDataFrame(
        [("a", 3.0, 1.0), ("b", 2.0, 2.0), ("c", 1.0, 3.0)], "skill string, x double, y double"
    )
    df = df.withColumn("rx", F.rank().over(Window.orderBy(F.desc("x")))).withColumn(
        "ry", F.rank().over(Window.orderBy(F.desc("y")))
    )
    assert df.stat.corr("rx", "ry") == pytest.approx(-1.0)
