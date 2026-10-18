# Project Proposal — Job Market Trends and Emerging Skills

**Course:** CSE412 Big Data & Large-Scale Computing — Applied Project
**Tool combination:** Track E — **HDFS → Apache Pig → Apache Hive → Apache Spark (MLlib)**

## 1. Problem statement

Employers' skill requirements change faster than curricula or career advice can
track them. Counting how often a skill appears in job advertisements cannot
distinguish a skill that is already ubiquitous from one that is genuinely
becoming important: SQL is mentioned in roughly half of all postings because it
is established, not because it is growing. Ranking by total frequency therefore
tells a student or a curriculum committee almost nothing about where demand is
*heading*.

This project asks: **can a job-posting corpus be processed at scale to rank
skills by how much they are emerging — separating genuine growth from raw
popularity — using demand-share change, trend consistency, occupational spread,
and changing skill co-occurrence?**

## 2. Dataset

`lukebarousse/data_jobs` from Hugging Face — **Apache-2.0**, no account required,
downloaded and measured (see `docs/dataset_audit.md`):

- **785,741 postings**, 17 columns, 231,152,089 bytes
- **12 clean monthly periods**: 2023-01 → 2023-12, no gaps
- **85.1%** of postings carry a structured skill list; **252 distinct skills**,
  **3,660,283** skill mentions, 0 parse failures
- **10 job-title categories** (→ 7 occupations + seniority), 160 countries
- Salary on only 2.8% of rows — salary is analysed separately, not folded into the
  index
- Duplicate rate under the composite key: **0.11%** (873 rows)

The 12-month panel is the reason this dataset was chosen. Alternatives examined
had a single-day scrape (six days of January), or 88 daily snapshots where 99.3%
of postings recurred the next day — neither supports period-over-period analysis.

## 3. Tools and why each is load-bearing

| Stage | Tool | What only it does here |
|---|---|---|
| Storage | **HDFS** | One durable landing zone for raw → cleaned → warehouse; the same Parquet files are read by both Hive and Spark |
| Batch ETL | **Apache Pig** | Declarative transformation of the raw CSV: type casting, date/period derivation, composite-key deduplication, seniority split, and exploding 785,741 postings into **3,660,283 job-skill rows** |
| Warehouse | **Apache Hive** | SQL access over the cleaned Parquet, partitioned by period — and an *independent* audit that Pig's cleaning actually worked (nulls, duplicates, period counts) |
| Analytics | **Apache Spark** | Distributed aggregation where the data is widest: skill × period × occupation cells, co-occurrence pairs, ESI component scoring, OLS trend slopes |
| ML | **Spark MLlib** | Salary regression with a time-based split — a technique neither Pig nor Hive provides |

This satisfies the course's scope test on two of its three grounds: the need for
**both batch and query-style access** (Pig vs Hive), and the need for an **ML
technique** (MLlib).

## 4. Method (summary)

1. **Demand share** `D(s,t) = postings mentioning s in t / all postings in t`; change
   `ΔD = D(s,t) − D(s,t−1)`; relative growth only where the prior share clears a
   documented floor (no infinite growth).
2. **Trend consistency** = OLS slope of a skill's monthly demand share across the
   12 periods.
3. **Occupational adoption** = distinct occupations requiring the skill / eligible
   observed occupations, with the denominator monitored so coverage change is not
   mistaken for spreading. Reported in its own table; **not scored** — with 7
   occupations it saturates at 1.0 and carries no information.
4. **Co-occurrence change** = Jaccard **and** PPMI over skill pairs, restricted to
   pairs meeting a minimum support threshold.
5. **Emerging Skill Index** = the three discriminating components (growth, trend,
   co-occurrence) percentile-normalised and combined with equal weights (0.333 each),
   with a weight-sensitivity grid and a comparison against frequency-only and
   growth-only rankings.
6. **Salary** analysed separately (medians with sample counts, plus a
   LinearRegression → GBT regression on a time-based split, reported with RMSE,
   MAE, R² against a mean-predictor baseline).

## 5. Expected outcome

- A reproducible pipeline: `download → HDFS → Pig → Hive → Spark → warehouse → dashboard`
  driven by `scripts/run_pipeline.sh`
- 12 periods of skill demand, co-occurrence and adoption tables in Hive/Parquet
- A ranked Emerging Skill Index with **component scores visible** for every skill,
  so a ranking can be explained rather than asserted
- A sensitivity analysis showing whether the top-ranked skills change under
  different weights
- A Streamlit dashboard (3 pages) reading only precomputed outputs
- Honest documentation: measured results, data-quality findings, and limitations

## 6. Scope and non-goals

**In scope:** the five features above, one dataset, the native Hadoop/Pig/Hive/Spark
installation already running on this machine.

**Out of scope, deliberately:** GraphX, Kafka/streaming, Docker, LLM-based skill
extraction (the dataset ships structured skills), cloud deployment, publication.

## 7. Known risks

| Risk | Mitigation |
|---|---|
| Salary coverage 2.8% | Report descriptively with n-counts; keep it out of the index; document disclosure bias |
| 2023-only data | Frame findings as a 2023 analysis; the pipeline is the transferable asset |
| Country coverage artifacts (Sudan 21,781) | Apply a low-volume threshold and flag the artifact in the audit |
