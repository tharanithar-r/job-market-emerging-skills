# Analysing Job Market Trends and Emerging Skills Using Big Data

**CSE412 — Big Data & Large-Scale Computing · Applied Project**
Tools: HDFS · Apache Pig · Apache Hive · Apache Spark (MLlib)

---

## 1. Introduction

Skill requirements in the labour market change faster than curricula, career advice or hiring plans can
track them. The obvious analytical move — count how often each skill appears in job advertisements and
rank them — fails at precisely the question people actually care about. `sql` appears in 46% of the
postings analysed here and `python` in 47%, but that tells us they are **established**, not that they are
**growing**. A skill with a small total but a rapidly rising share, spreading across occupations and
forming new combinations with other skills, is the one a student or a curriculum committee needs to know
about, and a frequency table cannot show it.

This project builds an end-to-end distributed pipeline over 785,741 job postings and answers:

> **Can a job-posting corpus be processed at scale to rank skills by how much they are *emerging*,
> rather than by how often they appear?**

The approach combines three discriminating indicators — demand-share growth, trend consistency and
changing skill co-occurrence — into an Emerging Skill Index (ESI), and then tests whether that index
actually changes the answer compared with simpler rankings.

---

## 2. Dataset

`lukebarousse/data_jobs` (Hugging Face, Apache-2.0, direct download, no account required). The dataset
was **measured**, not taken on trust — full audit in `docs/dataset_audit.md`.

| Property | Measured |
|---|---|
| Rows / columns | 785,741 / 17 |
| File | 231,152,089 bytes, SHA-256 verified |
| Time coverage | 2023-01-01 → 2023-12-31, **12 monthly periods, no gaps** |
| Skills | structured lists on **85.1%** of postings; **252 distinct skills**, 0 parse failures |
| Occupations | 10 title categories → **7 occupations + seniority** |
| Countries | 160 |
| Salary | **2.8%** disclosure (`salary_year_avg`) |
| Duplicates | 10,465 (1.33%) under the composite key |

**Why this dataset.** Three alternatives were evaluated in depth and rejected on measured grounds:

- `asaniczka/1-3m-linkedin` (1.35M rows) is labelled "2024" but its `first_seen` values span
  **2024-01-12 → 2024-01-17** — six days. No period-over-period analysis is possible.
- `edwarddgao/open-apply-jobs` (61.5M rows, MIT) has real scale and a genuine four-month daily panel,
  but **99.3% of postings recur the next day** because it snapshots *open* roles rather than new ones,
  and its skills must be extracted from HTML — an error-prone layer this project did not need.
- `techmap` / MCM monthly postings are CC BY-**ND**; a project whose entire output is derived data
  cannot honour a NoDerivatives licence.

The chosen dataset is the only candidate with **both** a clean multi-period panel and structured skill
labels.

---

## 3. Architecture

```
data_jobs.csv → HDFS raw → Pig ETL → HDFS cleaned (Parquet) → Hive warehouse → Spark → dashboard
```

Each tool does work the others cannot:

| Tool | Contribution |
|---|---|
| **HDFS** | Durable landing zone for raw → cleaned → warehouse; the same Parquet files are read by Hive and Spark, so there is one copy of the data and any engine can be replaced independently |
| **Apache Pig** | The batch ETL: quoted-CSV parsing, type and date casting, validation filtering, composite-key deduplication, seniority splitting, and exploding 775,275 postings into **3,608,325 job-skill rows** |
| **Apache Hive** | Query-style access and an **independent audit** of Pig's cleaning (nulls, duplicates, period counts), plus a CTAS that computes `skill_demand` for the next stage to consume |
| **Apache Spark** | Distributed aggregation where the data is widest: skill × period features, pair metrics, percentile normalisation, and MLlib |
| **Spark MLlib** | Salary regression with a time-based split — the ML technique Pig and Hive cannot provide |

**Handoff design.** Hive and Spark deliberately do **not** share a metastore: Spark 4.2 bundles Hive
2.3.10 with Derby 10.16.1.1 while Hive 4.2.1 uses Derby 10.17.1.0, and pointing both at one embedded
metastore fails at startup. The contract between engines is instead **Parquet on HDFS** — Hive declares
external tables over Pig's output, and Spark reads the same paths. This is a design decision with a
justification, not a workaround.

---

## 4. Methodology

**Demand share.** `D(s,t) = postings mentioning s in t / all postings in t`. Share rather than count, so
that a growing corpus does not masquerade as growing demand. Change is `ΔD = D(t) − D(t−1)`; relative
growth is emitted only where the previous share clears a floor, so a newly observed skill never produces
infinite growth.

**Trend consistency.** OLS slope of a skill's 12 monthly demand shares, computed from a single
`groupBy` of `n, Σx, Σy, Σxy, Σx²`.

**Co-occurrence.** For each pair: `Jaccard = c/(a+b−c)` and `PPMI = max(0, log₂(c·N/(a·b)))`. Both are
reported because they fail in opposite directions — Jaccard is inflated by ubiquitous skills, PPMI is
biased toward rare pairs. Skills need ≥ 50 postings and pairs ≥ 50 postings to be stored (223 of 252
skills retained).

**Emerging Skill Index.** Each component is **percentile-ranked** across eligible skills so that
incompatible units become comparable ranks, then combined with equal weights:

```
ESI = ⅓·G + ⅓·T + ⅓·C
  G = ΔD in the final period
  T = OLS slope over 12 periods
  C = change in mean PPMI of the skill's pairs (final minus first period)
```

Percentile rank was chosen over min–max because a single outlier would otherwise compress every other
skill into a narrow band.

**Two components were deliberately excluded**, and the exclusions are measured rather than assumed:

- **Salary** covers 2.8% of postings; an S component would be null or noise for most skills.
- **Occupational adoption** saturates: with 7 occupations, nearly every skill appears in all of them. In
  the four-component version of the index every top-15 skill scored *exactly* 0.4054 on it — a constant
  component dilutes the weights of the components that do discriminate.

Both are still computed and reported in their own tables, so a reader can verify the claim.

**Validation.** The ESI is compared against a frequency-only ranking, a growth-only ranking, and four
weightings (equal, growth-heavy, trend-heavy, co-occurrence-heavy).

---

## 5. Results

### 5.1 What is simply common

December 2023, share of postings: python 47.2%, sql 46.3%, aws 18.2%, azure 16.8%, excel 15.1%,
r 14.9%, tableau 14.8%, spark 13.4%, power bi 12.4%, java 10.2%.

This is the ranking a frequency table gives, and it is the ranking that motivated the project.

### 5.2 What is emerging

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

### 5.3 The index changes the answer

| Comparison | Top-10 overlap | Spearman ρ (223 skills) |
|---|---:|---:|
| ESI vs frequency-only ranking | **1 / 10** | **0.333** |
| ESI vs growth-only ranking | **1 / 10** | **0.699** |
| frequency vs growth-only | — | **0.193** |

Only `power bi` appears in all three top-10s. This is the project's central result: **ranking by
popularity and ranking by emergence produce almost disjoint answers.**

The Spearman coefficients quantify it across all 223 skills rather than just the top ten. The most
telling figure is the third row: the correlation between how *common* a skill is and how much it is
*growing* is **0.193** — close to no relationship at all. A frequency ranking is not a noisy proxy for an
emerging-skills ranking; it is essentially a different measurement. That is the empirical justification
for the project's premise.

A student acting on the frequency ranking would prioritise python and sql; a student acting on the ESI
would also consider fastapi, pytorch and hugging face.

`hugging face` is the clearest illustration of why the dimensions are combined rather than used singly.
Its absolute share is weak (G = 0.428) but it has the strongest co-occurrence change of any skill
(C = 1.000) and a near-top trend (T = 0.977). A low-volume skill whose *relationships* are shifting is
exactly the pattern a frequency table cannot represent.

### 5.4 Robustness

Top-10 overlap with the equal-weight ranking: growth-heavy 0.8, trend-heavy 0.8, co-occurrence-heavy
**0.6**. The ranking is stable under reasonable reweighting, and the lower figure confirms the
co-occurrence dimension is the most independent of the three — which is the reason for including it.

### 5.5 Salary

Median advertised annual salary (n ≥ 100, annual contracts only): Data Scientist $134,814 (n = 7,449) ·
Data Engineer $132,978 (5,955) · Machine Learning Engineer $106,000 (572) · Software Engineer $98,578
(462) · Data Analyst $92,500 (6,486) · Business Analyst $84,944 (609).

---

## 6. Evaluation

### 6.1 ETL correctness

Hive independently re-derived every count from the Parquet and matched Pig's own counters:

| Check | Value |
|---|---|
| postings | 775,275 |
| distinct periods | 12 |
| duplicate job keys | **0** |
| empty job titles | 0 |
| job-skill rows | 3,608,325 |
| distinct skills | 252 |
| distinct occupations | 7 |
| postings with salary | 21,598 |
| average skills per posting | 5.47 |

The one row lost before deduplication (785,741 → 785,740) is accounted for: a posting with no
`job_title`. Nothing else was dropped without a recorded reason.

### 6.2 Model performance

Train Jan–Sep 2023 (17,823 rows), test Oct–Dec 2023 (3,775 rows) — time-based, not random.

| Model | RMSE | MAE | R² |
|---|---:|---:|---:|
| mean baseline | 51,343 | 38,901 | −0.001 |
| linear regression | 44,023 | 31,719 | 0.264 |
| gradient-boosted trees | **44,006** | **31,515** | **0.264** |

Both models beat the baseline by ~14% on RMSE. GBT and linear regression are effectively tied
(44,006 vs 44,023 — a 0.04% difference), which is reported rather than dressed up: with 17.8k training
rows and mostly categorical features, the linear model already captures what the trees can add.

### 6.3 Tests

15 tests pass (`pytest`): 8 metric tests with hand-computed expected values (demand share, share change,
guarded relative growth, division-by-zero behaviour under ANSI mode, Jaccard, PPMI, PPMI clipping,
adoption share), 5 ESI tests (percentile rank monotonicity, unit range, missing-component
renormalisation, `components_available`, rank ordering), and 2 dashboard tests that drive all three pages
through Streamlit's `AppTest` and assert the KPIs equal the real pipeline counts.

### 6.4 End-to-end

`./scripts/run_pipeline.sh` executes all six stages from a clean state and reproduces every output.
Stage wall-clock times are in `docs/evaluation.md`.

---

## 7. Challenges

The implementation surfaced problems that cost real time and are documented rather than hidden:

1. **Quoted CSV in Pig.** `PigStorage(',')` splits inside quoted fields; `STRSPLIT` with a comma literal
   silently failed to split at all, and apostrophe escapes (`\x27`) did not resolve. The working recipe
   is `TOKENIZE(s, ',')` (which correctly keeps `"power bi"` intact) plus bracket-stripping and
   `SUBSTRING`.
2. **Classpath archaeology.** `CSVExcelStorage` needs `commons-collections` 3.x, and `ParquetStorer`
   needs `parquet-pig-bundle` — neither ships on Pig's default classpath.
3. **Parquet matched by column name.** After `FLATTEN(first)` in the dedup step, Pig wrote columns like
   `first::job_key`, so Hive silently read every row as NULL. Fixed at the source by re-projecting the
   fields, not by switching Hive to index-based access.
4. **Hive 4 without Tez.** Queries failed with `NoClassDefFoundError: org/apache/tez/...` until the
   engine was forced to MR; Hive also ignores JVM `-D` flags for job configuration, so every setting had
   to be an in-session `SET`.
5. **Spark 4 ANSI mode.** Division by zero now raises instead of returning NULL, so every division in
   the pipeline goes through `try_divide` and every cast through `try_cast`.
6. **A silent 0-row result.** `df.write.csv("outputs/...")` wrote to **HDFS**, because the default
   filesystem is HDFS — the dashboard read an empty directory until the path was forced to an absolute
   `file://` URI.

Each of these is a case where the failure mode was silent and the fix required reading the data rather
than the documentation.

---

## 8. Limitations

Stated fully in `docs/limitations.md`; the ones that most constrain the conclusions:

- **2023 only** — "emerging" means "emerging during 2023"; the pipeline is the reusable asset.
- **Advertised data/IT roles only** — not the labour market.
- **7 occupations** — adoption saturates, which is why it is not scored.
- **Salary disclosure 2.8% and non-random** — descriptive, never causal; R² 0.264 leaves most variance
  unexplained.
- **Sudan artifact** — 21,781 postings, implausible, treated as a geo-tagging artifact.
- **Closed skill vocabulary** — 252 tags; a genuinely new skill outside it is invisible.
- **No volume claim** — 231 MB does not require a cluster; the justification is batch + query-style
  access and the expansion to 3.6M derived rows.

---

## 9. Individual contribution

*To be completed by the team before submission.* Course requirements specify three students per team and
a per-person breakdown.

| Member | Areas |
|---|---|
| TBD | |
| TBD | |
| TBD | |

---

## 10. Conclusion

The project asked whether large-scale job-posting data can rank skills by emergence rather than
popularity. It can, and the difference is not marginal: the ESI shares **one skill out of ten** with a
frequency ranking and one out of ten with a growth-only ranking. The three-component index — demand
growth, OLS trend and co-occurrence change — surfaces `fastapi`, `pytorch`, `matplotlib`, `django` and
`hugging face`, none of which a popularity table would recommend, while remaining stable under
reweighting (top-10 overlap 0.6–1.0).

Two methodological exclusions were made on measured grounds rather than convenience: salary, because
2.8% coverage makes it non-representative, and occupational adoption, because it is constant across
skills at this granularity. Both are still reported so the reasoning can be checked.

The pipeline itself is the reusable asset: 785,741 raw postings → 775,275 deduplicated → 3,608,325
job-skill rows, reconciled at every stage, validated by an independent engine, tested with hand-computed
examples, and presented through a dashboard that reads only real outputs.
