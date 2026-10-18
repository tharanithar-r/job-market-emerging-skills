"""Job Market Trends & Emerging Skills - dashboard.

Reads only the precomputed outputs in outputs/spark/. It never runs Spark or the pipeline,
so changing a filter cannot trigger recomputation.

Run:  .venv/bin/streamlit run dashboard/app.py
"""
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

OUT = Path(__file__).resolve().parent.parent / "outputs" / "spark"


@st.cache_data
def load(name: str) -> pd.DataFrame:
    """Spark writes each CSV as a directory containing one part file."""
    d = OUT / name
    parts = sorted(d.glob("part-*.csv")) if d.is_dir() else []
    return pd.read_csv(parts[0]) if parts else pd.DataFrame()


st.set_page_config(page_title="Job Market Trends & Emerging Skills", layout="wide")

PAGES = ["Overview & Data Quality", "Skill Trends", "Emerging Skills"]
page = st.sidebar.radio("Page", PAGES)

trends = load("skill_trends_all.csv")
esi = load("esi_top50.csv")
pairs = load("top_pairs.csv")
model_metrics = load("salary_metrics.csv")
quality = load("pipeline_quality.csv")
by_occupation = load("salary_by_occupation.csv")
by_skill = load("salary_by_skill.csv")
sensitivity = load("esi_sensitivity.csv")
baselines = load("esi_baselines.csv")

if trends.empty:
    st.error("No outputs in outputs/spark/. Run ./scripts/run_spark.sh first.")
    st.stop()

PERIODS = sorted(trends["period"].unique())
LAST = PERIODS[-1]
counts = quality.set_index("stage")["records"] if not quality.empty else pd.Series(dtype="int64")


def stage(name: str, default: int = 0) -> int:
    return int(counts.get(name, default))


# --------------------------------------------------------------------------- #
if page == PAGES[0]:
    st.title("Job Market Trends & Emerging Skills")
    st.caption(
        "2023 job postings, analysed end to end: HDFS → Apache Pig → Apache Hive → Apache Spark."
    )

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Postings analysed", f"{stage('after_dedup'):,}")
    c2.metric("Monthly periods", len(PERIODS))
    c3.metric("Distinct skills", f"{trends['skill_norm'].nunique():,}")
    c4.metric("Job-skill rows", f"{stage('job_skill_rows'):,}")

    st.subheader("Pipeline accounting")
    st.caption("Every stage reconciles; nothing was dropped silently.")
    if not quality.empty:
        st.dataframe(quality.sort_values("stage"), width="stretch")

    st.subheader("Salary model (time-based split: train Jan–Sep, test Oct–Dec)")
    st.dataframe(model_metrics, width="stretch")

    st.subheader("What this data can and cannot tell you")
    st.markdown(
        "- **Scope:** advertised data/IT roles in 2023, scraped from job boards — not the whole "
        "labour market. Blue-collar, informal and non-advertised roles are absent.\n"
        "- **Occupations:** 10 title categories, collapsed to 7 occupations plus seniority. "
        "Occupational adoption therefore saturates and is reported but not scored in the index.\n"
        "- **Salary:** only 2.8% of postings disclose it, so salary findings describe "
        "salary-disclosing postings. Associations are descriptive, never causal.\n"
        "- **Time:** 2023 only. This is a historical analysis; the pipeline is the reusable part.\n"
        "- **Country artifact:** Sudan appears with 21,781 postings — implausibly large, and "
        "treated as a geo-tagging artifact rather than a labour market."
    )

# --------------------------------------------------------------------------- #
elif page == PAGES[1]:
    st.title("Skill Trends")

    top_now = (
        trends[trends["period"] == LAST]
        .sort_values("posting_count", ascending=False)["skill_norm"]
        .head(5)
        .tolist()
    )
    picked = st.sidebar.multiselect("Skills to chart", sorted(trends["skill_norm"].unique()), top_now)

    if picked:
        chart = trends[trends["skill_norm"].isin(picked)].copy()
        chart["share_pct"] = chart["demand_share"] * 100
        fig = px.line(
            chart.sort_values("period"),
            x="period",
            y="share_pct",
            color="skill_norm",
            markers=True,
            labels={"period": "Month", "share_pct": "% of postings", "skill_norm": "Skill"},
            title="Demand share over time",
        )
        st.plotly_chart(fig, width="stretch")

    latest = trends[trends["period"] == LAST].sort_values("posting_count", ascending=False).head(15)
    fig2 = px.bar(
        latest,
        x="posting_count",
        y="skill_norm",
        orientation="h",
        labels={"posting_count": "Postings", "skill_norm": ""},
        title=f"Most requested skills — {LAST}",
    )
    fig2.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig2, width="stretch")

    st.subheader("Biggest share gains in the final period")
    st.dataframe(
        latest.assign(share_pct=(latest["demand_share"] * 100).round(2))[
            ["skill_norm", "posting_count", "share_pct", "demand_share_change", "occupation_count"]
        ],
        width="stretch",
    )

# --------------------------------------------------------------------------- #
else:
    st.title("Emerging Skills")
    st.caption(
        "ESI = equal-weight percentile ranks of demand growth (G), trend slope (T) and "
        "co-occurrence change (C). Salary and occupational adoption are reported separately."
    )

    top = esi.sort_values("rank").head(15)
    fig = px.bar(
        top,
        x="esi_score",
        y="skill_norm",
        orientation="h",
        labels={"esi_score": "ESI score", "skill_norm": ""},
        title="Top 15 emerging skills",
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, width="stretch")

    st.subheader("Component scores behind the ranking")
    st.caption("Percentile ranks: 1.0 is the highest-ranked skill on that component.")
    st.dataframe(
        esi.sort_values("rank")[
            ["rank", "skill_norm", "G", "T", "C", "components_available", "esi_score"]
        ].round(3),
        width="stretch",
    )

    left, right = st.columns(2)
    with left:
        st.subheader("Does the ranking depend on the weights?")
        st.dataframe(sensitivity, width="stretch")
    with right:
        st.subheader("ESI vs simpler rankings")
        st.dataframe(baselines, width="stretch")

    rel_tab, sal_tab = st.tabs(["Skill relationships", "Salary association"])

    with rel_tab:
        st.caption("Jaccard = set overlap; PPMI = more common than chance. Pairs need ≥50 postings.")
        st.dataframe(
            pairs.sort_values("cooccurrence_count", ascending=False).head(40).round(3),
            width="stretch",
        )

    with sal_tab:
        st.caption("Advertised annual salary. Descriptive only — no causal claim.")
        st.dataframe(by_occupation, width="stretch")
        st.dataframe(by_skill.head(20), width="stretch")
