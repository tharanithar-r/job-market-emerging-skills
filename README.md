# Job Market Trends and Emerging Skills

A distributed analytics pipeline that identifies **emerging skills** — not merely popular ones — from
job-posting data. Built for **CSE412 Big Data & Large-Scale Computing** using
**HDFS → Apache Pig → Apache Hive → Apache Spark (MLlib)**.

---

## 1. Problem statement

Employers' skill requirements change faster than curricula or career advice can track them.
Counting how often a skill appears cannot separate a skill that is *already ubiquitous* from one that
is *becoming important*: `sql` appears in 46% of postings because it is established, not because it is
growing. Ranking by frequency therefore tells a student almost nothing about where demand is heading.

The project asks: **can a job-posting corpus be processed at scale to rank skills by how much they are
emerging**, using demand growth, trend consistency and changing skill co-occurrence?

**Objectives**

1. Compute demand share and its change for every skill across 12 monthly periods.
2. Measure which skills are requested together, with Jaccard and PPMI.
3. Measure occupational breadth, controlling for changes in observed coverage.
4. Combine the discriminating indicators into an Emerging Skill Index (ESI) with visible component
   scores, and test how much the result depends on the weights.
5. Validate the index against frequency-only and growth-only baselines.
6. Present everything through a dashboard that reads only real pipeline outputs.

---

## 2. Architecture

```
data_jobs.csv
   │
   ▼
HDFS /jobmarket/raw/postings  ──  original file, unchanged
   │
   ▼
Apache Pig  ── parse, type-cast, deduplicate, split seniority, explode skills
   │            (785,741 rows → 775,275 postings → 3,608,325 job-skill rows)
   ▼
HDFS /jobmarket/cleaned/{postings,job_skills}   (Parquet)
   │
   ├──► Apache Hive  ── external tables + validation SQL + `skill_demand` CTAS
   │
   ▼
Apache Spark  ── demand trends, co-occurrence, adoption, ESI, MLlib salary model
   │
   ▼
HDFS /jobmarket/warehouse/*  +  outputs/spark/*.csv
   │
   ▼
Streamlit dashboard (3 pages)
```

**Why each tool is load-bearing**

| Tool | What only it does here |
|---|---|
| HDFS | Single durable landing zone for raw → cleaned → warehouse; the same Parquet files are read by both Hive and Spark |
| Pig | Declarative batch ETL: quoted-CSV parsing, composite-key deduplication, skill-string explosion (5.47 skills/posting) |
| Hive | Query-style access and an *independent* audit that Pig's cleaning worked; computes `skill_demand` |
| Spark | Distributed aggregation over 3.6M job-skill rows: skill × period cells, co-occurrence pairs, percentile normalisation |
| Spark MLlib | Salary regression with a time-based split — a technique Pig and Hive cannot provide |

Full diagram and data flow: [`docs/architecture.md`](docs/architecture.md).

---

## 3. Dataset

| Item | Value |
|---|---|
| Dataset | `lukebarousse/data_jobs` |
| Source | <https://huggingface.co/datasets/lukebarousse/data_jobs> |
| Access | Direct HTTPS download, no account or token |
| Licence | **Apache-2.0** |
| File | `data_jobs.csv`, **231,152,089 bytes**, UTF-8 CSV |
| SHA-256 | `635241ed09ccee18bdae1f83b45f26d6759e0aa2513c529f6190e9054062436c` |
| Rows / columns | **785,741** / 17 |
| Time coverage | 2023-01-01 → 2023-12-31, **12 monthly periods**, no gaps |
| Skills | structured lists on **85.1%** of postings; **252 distinct skills** |
| Occupations | 10 `job_title_short` values → **7 occupations + seniority** |
| Salary | only **2.8%** of postings (`salary_year_avg`), 21,598 rows after cleaning |
| Duplicates | 10,465 (1.33%) at date-level composite key |

All figures are **measured**, not quoted from the provider. Full audit with missing-value rates,
coverage artifacts and limitations: [`docs/dataset_audit.md`](docs/dataset_audit.md).

> **Why this dataset:** it is the only candidate examined with *both* a clean multi-period panel and
> structured skill labels. Alternatives had a six-day scrape window (`asaniczka`, 1.35M rows) or 88
> daily snapshots where 99.3% of postings recurred the next day (`open-apply-jobs`, 61.5M rows).
> Neither supports period-over-period comparison.

---

## 4. Prerequisites (verified versions)

| Component | Version | Notes |
|---|---|---|
| Hadoop / HDFS | **3.4.3** | `/opt/hadoop-3.4.3`, HDFS daemons running |
| Apache Pig | **0.18.0** | `/opt/pig`; needs `piggybank.jar`, `parquet-pig-bundle`, `commons-collections-3.2.2` |
| Apache Hive | **4.2.1** | `/opt/apache-hive-4.2.1-bin`; embedded Derby metastore |
| Apache Spark | **4.2.0** (Scala 2.13) | Homebrew `apache-spark` |
| Java | **21** | `JAVA_HOME` must point at JDK 21 |
| Python | **3.13** | project venv; MLlib requires `numpy` |
| Streamlit / Plotly | 1.66 / 7.1 | dashboard only |

**Setup**

```bash
uv venv --python 3.13 .venv
uv pip install --python .venv/bin/python -r requirements.txt
```

---

## 5. Running the pipeline

Every stage is idempotent.

```bash
./scripts/run_pipeline.sh          # all six stages
```

Or individually:

| Step | Command | Produces |
|---|---|---|
| 1 | `./scripts/download_data.sh` | `data/raw/data_jobs.csv`, checksum verified |
| 2 | `python3 scripts/validate_dataset.py` | measured dataset profile (stdout) |
| 3 | `./scripts/upload_to_hdfs.sh` | `/jobmarket/raw/postings/`, row count reconciled |
| 4 | `./scripts/run_pig.sh` | `/jobmarket/cleaned/{postings,job_skills}` (Parquet) |
| 5 | `./scripts/run_hive.sh` | `jobmarket` database, `skill_demand`, validation output |
| 6 | `./scripts/run_spark.sh` | `/jobmarket/warehouse/*` + `outputs/spark/*.csv` |

**Dashboard**

```bash
.venv/bin/streamlit run dashboard/app.py
```

**Tests**

```bash
.venv/bin/python -m pytest tests -q     # 15 tests: metrics, ESI, dashboard rendering
```

### Environment quirks this repo already handles

These cost real time to discover; each is handled in the scripts with the reason documented.

| Quirk | Handling |
|---|---|
| Pig cannot parse quoted CSV with core loaders | `CSVExcelStorage` from `piggybank.jar` |
| `CSVExcelStorage` needs `commons-collections` 3.x | added to `PIG_CLASSPATH` |
| `ParquetStorer` not on Pig's default classpath | `parquet-pig-bundle-1.9.0.jar` added |
| Pig/Hive default scratch dirs unwritable in restricted shells | explicit `hadoop.tmp.dir` / `mapred.local.dir` |
| YARN ResourceManager cannot start here | `mapreduce.framework.name=local` over HDFS |
| Hive 4 defaults to Tez (not installed) | `SET hive.execution.engine=mr` |
| Hive ignores JVM `-D` for job config | settings passed as in-session `SET` |
| Beeline dies without a TTY | `TERM=dumb` |
| `beeline -f` echoes without executing | statements passed with `-e` |
| Beeline client heap defaults to 256 MB | `HADOOP_HEAPSIZE_MAX=4096` |
| Hive builds a literal `libjars/*` glob | `mkdir libjars` before running |
| Hive matches Parquet **by column name** | Pig re-projects fields to strip `first::` prefixes |
| Spark 4 enables ANSI mode by default | all divisions use `try_divide`, casts use `try_cast` |
| Spark resolves relative paths against HDFS | `write_csv` forces an absolute `file://` URI |
| MLlib needs numpy | venv interpreter used for driver and workers |

---

## 6. Methodology (summary)

**Demand share** for skill *s* in period *t*: `D(s,t) = postings mentioning s in t / all postings in t`.
Change is `ΔD = D(s,t) − D(s,t−1)`. Relative growth is reported only where the previous share clears a
floor, so a skill observed for the first time never produces infinite growth.

**Trend consistency**: OLS slope of a skill's 12 monthly demand shares.

**Co-occurrence**: for each pair, `Jaccard = c / (a + b − c)` and `PPMI = max(0, log₂(c·N / (a·b)))`.
Jaccard is inflated by ubiquitous skills; PPMI punishes rare ones — they disagree on purpose, so both
are reported.

**Emerging Skill Index** — three discriminating components, each percentile-ranked across eligible
skills, combined with equal weights (0.333):

```
ESI = ⅓·G + ⅓·T + ⅓·C
  G = demand-share change in the final period
  T = OLS slope of monthly demand share
  C = change in mean PPMI of the skill's pairs, final period minus first
```

Occupational adoption and salary are **deliberately excluded from the index** and reported separately
(see `docs/methodology.md` §6 for why). Full derivation, normalisation and weight-sensitivity method:
[`docs/methodology.md`](docs/methodology.md).

---

## 7. Results

**Top skills by demand share, December 2023** — python 47.2%, sql 46.3%, aws 18.2%, azure 16.8%,
excel 15.1%, r 14.9%, tableau 14.8%, spark 13.4%, power bi 12.4%, java 10.2%.

**Emerging Skill Index, top 10**

| Rank | Skill | G | T | C | ESI |
|---:|---|---:|---:|---:|---:|
| 1 | fastapi | 0.806 | 0.937 | 0.950 | 0.898 |
| 2 | pytorch | 0.932 | 1.000 | 0.712 | 0.881 |
| 3 | phoenix | 0.883 | 0.950 | 0.734 | 0.856 |
| 4 | matplotlib | 0.662 | 0.986 | 0.896 | 0.848 |
| 5 | power bi | 0.986 | 0.982 | 0.545 | 0.838 |
| 6 | dax | 0.797 | 0.910 | 0.788 | 0.832 |
| 7 | django | 0.752 | 0.730 | 0.968 | 0.817 |
| 8 | databricks | 0.820 | 0.991 | 0.617 | 0.809 |
| 9 | hugging face | 0.428 | 0.977 | 1.000 | 0.802 |
| 10 | ubuntu | 0.455 | 0.964 | 0.910 | 0.776 |

**The index changes the answer.** Overlap with the frequency ranking: **1/10**. Overlap with a
growth-only ranking: **1/10**. Across all 223 scored skills the Spearman correlation is **0.333** with
frequency and **0.699** with growth — while the correlation between frequency and growth themselves is
only **0.193**. Popularity and growth are nearly unrelated, which is exactly why a frequency ranking
cannot answer an emergence question. `hugging face` is the clearest case — weak absolute share
(G = 0.428) but the strongest co-occurrence change of any skill (C = 1.000), which no frequency table can
surface.

**Weight sensitivity** — top-10 overlap with the equal-weight ranking: growth-heavy 0.8, trend-heavy
0.8, co-occurrence-heavy 0.6. The ranking is stable, and the co-occurrence dimension is the most
independent of the three.

**Salary model** (train Jan–Sep 2023, test Oct–Dec 2023; 17,823 / 3,775 rows)

| Model | RMSE | MAE | R² |
|---|---:|---:|---:|
| mean baseline | 51,343 | 38,901 | −0.001 |
| linear regression | 44,023 | 31,719 | 0.264 |
| gradient-boosted trees | **44,006** | **31,515** | **0.264** |

Detailed results, reconciliation and per-stage timings: [`docs/evaluation.md`](docs/evaluation.md).

---

## 8. Example questions this answers

- *Which skills are growing rather than merely common?* → ESI page; `fastapi`, `pytorch`, `hugging face`
  rank high while `python`/`sql` do not, despite far larger totals.
- *What is requested alongside Python?* → Skill Relationships tab: pair counts with Jaccard and PPMI.
- *Is a skill spreading across roles?* → `occupational_adoption` table (7 occupations).
- *Does a skill pay more?* → Salary tab: median advertised salary per occupation and per skill, with
  sample counts; explicitly descriptive.
- *Can I trust the numbers?* → Overview page reconciles every stage: 785,741 raw → 785,740 filtered →
  775,275 deduplicated → 3,608,325 job-skill rows.

---

## 9. Known limitations

1. **2023 only.** This is a historical analysis, not a live signal. The pipeline is the reusable asset.
2. **Advertised roles only.** Online data/IT postings; blue-collar, informal and non-advertised work is absent.
3. **Occupational granularity.** 7 occupations after splitting seniority, so adoption saturates at 1.0 and
   is reported but not scored.
4. **Salary self-selection.** 2.8% disclosure; salary findings describe salary-disclosing postings and are
   descriptive, never causal.
5. **Geographic artifact.** Sudan appears with 21,781 postings — implausible, treated as a geo-tagging artifact.
6. **Closed vocabulary.** 252 curated skills: "never tagged" and "no longer emerging" are indistinguishable.
7. **Volume.** 231 MB / 785,741 rows. The big-data case rests on batch + query-style access and the
   expansion to 3.6M derived rows, not on source volume.
8. **Single node.** HDFS replication 1; no distributed-speedup claim is made.

Full discussion: [`docs/limitations.md`](docs/limitations.md).

---

## 10. Repository layout

```
config/project.yaml        paths, thresholds, locked analysis decisions
scripts/                   download · validate · upload · run_pig · run_hive · run_spark · run_pipeline
pig/                       clean_postings.pig · build_job_skills.pig
hive/                      create_tables.sql · validation_queries.sql · analytical_queries.sql
spark/                     common.py · skill_trends · skill_cooccurrence · occupational_adoption
                           · emerging_skill_index · salary_model
dashboard/app.py           3-page Streamlit app over outputs/spark/*.csv
tests/                     test_metrics · test_esi · test_dashboard
docs/                      dataset_audit · architecture · methodology · evaluation · limitations
                           · decision_log · proposal · implementation_plan · demo_script
report.md                  6–10 page submission report
```

### Design decisions and rejected alternatives

[`docs/decision_log.md`](docs/decision_log.md) records every locked choice **and** the alternatives
rejected, including why GraphX, Kafka, Docker and salary-inside-the-index were not used.
