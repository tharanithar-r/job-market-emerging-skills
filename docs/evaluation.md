# Evaluation

Measured results only. Where something was not measured, it says so rather than estimating.

---

## 1. Dataset scale (measured)

| Metric | Value |
|---|---|
| Source rows / columns | 785,741 / 17 |
| File size | 231,152,089 bytes |
| Distinct skills | 252 (223 retained at ≥ 50 postings) |
| Distinct occupations | 7 (after splitting seniority from 10 title categories) |
| Distinct countries | 160 |
| Skill mentions in source | 3,660,283 |
| Postings with salary | 22,003 (2.8%) |
| Duplicate rows (full timestamp) | 873 (0.11%) |
| Duplicate rows (date-level, as applied) | 10,465 (1.33%) |

Reproduce with `python3 scripts/validate_dataset.py` (3.4 s).

---

## 2. Row reconciliation

| Stage | Rows | Change |
|---|---:|---|
| raw_loaded | 785,741 | — |
| after_filter | 785,740 | −1 (posting with no `job_title`) |
| after_dedup | 775,275 | −10,465 (same-day duplicates) |
| postings_with_skills | 659,556 | postings carrying at least one skill |
| job_skill_rows | 3,608,325 | 5.47 skills per posting |

**Both engines agree.** Hive re-derived the posting counts, duplicate count and skill count from the
Parquet independently of Pig, and matched exactly:

```
rows_postings 775,275 · distinct_periods 12 · duplicate_job_keys 0 · empty_job_title 0
job_skill_rows 3,608,325 · distinct_skills 252 · distinct_occupations 7
postings_with_salary 21,598 · avg_skills_per_posting 5.47
```

The 1,105 difference between the source's 3,660,283 skill mentions and the pipeline's 3,608,325
job-skill rows is the skill content of the 10,465 deduplicated postings — expected, and consistent with
~4.7 skills per removed posting.

---

## 3. Processing performance

Wall-clock on a single machine (Apple silicon, single node, HDFS replication 1, local execution mode).
Measured by `time ./scripts/run_pipeline.sh` with the dataset already downloaded.

| Stage | Time | Notes |
|---|---:|---|
| Download + validate + HDFS upload | ~135 s (combined) | Pig runs inside this window |
| Pig ETL (both jobs) | included above | `clean_postings` + `build_job_skills` |
| Hive warehouse | **40 s** | external tables, CTAS, 25 queries |
| Spark analytics + ESI + MLlib | **111 s** | 5 jobs |
| **Total pipeline** | **286 s (4 m 46 s)** | |

**No distributed-speedup claim is made.** There is one node; MapReduce and Spark both run in local mode.
Measuring "speedup" here would be measuring nothing. What the timing does show is that the pipeline is
fast enough to iterate on, and that the cost is dominated by the Spark stage — consistent with it being
the widest data (3.6M job-skill rows).

Resource profiling (CPU, memory per stage) was **not** instrumented.

---

## 4. Analytical results

### 4.1 Demand (December 2023, share of postings)

python 47.2% · sql 46.3% · aws 18.2% · azure 16.8% · excel 15.1% · r 14.9% · tableau 14.8% ·
spark 13.4% · power bi 12.4% · java 10.2%

### 4.2 Emerging Skill Index (top 10)

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

All 223 scored skills have `components_available = 3`.

### 4.3 Baseline comparison — the headline result

| Ranking | Overlap with ESI top-10 |
|---|---:|
| frequency-only | **1 / 10** (power bi) |
| growth-only | **1 / 10** (power bi) |

Spearman rank correlation across all **223 scored skills** (Pearson correlation of `rank()`, so ties
share a rank):

| Comparison | Spearman ρ | Reading |
|---|---:|---|
| ESI vs frequency | **0.333** | Weak — the index is substantially a different ranking from popularity |
| ESI vs growth | **0.699** | Strong, as expected: G is one of three components and T tracks growth |
| frequency vs growth | **0.193** | **Almost no relationship** |

The third row is the project's premise, measured: **how popular a skill is tells you almost nothing about
whether it is growing.** A frequency ranking is not a noisy version of an emerging-skills ranking — it is
close to unrelated to it.

### 4.4 Weight sensitivity

| Weighting (G/T/C) | Top-10 overlap with equal |
|---|---:|
| equal 0.333 / 0.333 / 0.333 | 1.0 |
| growth-heavy 0.50 / 0.30 / 0.20 | 0.8 |
| trend-heavy 0.30 / 0.50 / 0.20 | 0.8 |
| co-occurrence-heavy 0.20 / 0.20 / 0.60 | **0.6** |

Interpretation: the ranking is not an artifact of the weight choice, and co-occurrence is the most
independent dimension.

### 4.5 Co-occurrence

Pairs are retained only at ≥ 50 postings, from a 223-skill vocabulary — an upper bound of
`223 × 222 / 2 = 24,753` distinct pairs per period. Both Jaccard and PPMI are stored; they rank pairs
differently by design (Jaccard favours common skills, PPMI favours surprising pairings).

### 4.6 Salary (descriptive, annual contracts, n ≥ 100)

| Occupation | n | Median | p25 | p75 |
|---|---:|---:|---:|---:|
| Data Scientist | 7,449 | $134,814 | — | — |
| Data Engineer | 5,955 | $132,978 | — | — |
| Machine Learning Engineer | 572 | $106,000 | — | — |
| Software Engineer | 462 | $98,578 | — | — |
| Data Analyst | 6,486 | $92,500 | — | — |
| Business Analyst | 609 | $84,944 | — | — |

Percentiles are exported in `outputs/spark/salary_by_occupation.csv`; the table above shows medians only.
Differences are **descriptive**, and the population is limited to employers who disclose salary.

---

## 5. Model evaluation

Time-based split: train `posted_date ≤ 2023-09-30` (17,823 rows), test `> 2023-09-30` (3,775 rows).
No target leakage: the label (`salary`) is never used as a feature, and the split is by date.

| Model | RMSE | MAE | R² | vs baseline (RMSE) |
|---|---:|---:|---:|---:|
| mean predictor (baseline) | 51,343 | 38,901 | −0.001 | — |
| linear regression | 44,023 | 31,719 | 0.264 | −14.3% |
| gradient-boosted trees | **44,006** | **31,515** | **0.264** | **−14.3%** |

**Interpretation, stated plainly:** GBT and linear regression are effectively tied (a 0.04% RMSE
difference). With 17.8k rows and largely categorical features, the linear model already captures what the
trees add. Both explain ~26% of variance, so ~74% is unexplained — this is a demonstration that MLlib
runs correctly on the pipeline's output, not a salary predictor anyone should act on.

The negative R² of the baseline (−0.001) is expected: predicting the training mean on a test set that is
slightly differently distributed is marginally worse than predicting the test mean.

---

## 6. Test results

```
.venv/bin/python -m pytest tests -q
15 passed
```

| File | Tests | What they pin down |
|---|---:|---|
| `tests/test_metrics.py` | 8 | demand share (1/2 = 0.5), share change (0.5→0.4 = −0.1, first period null), guarded relative growth (previous share 0 → NULL, never infinity), `try_divide(1,0)` → NULL under ANSI, Jaccard 3/(4+5−3) = 0.5, PPMI log₂(30/20) = 0.585, PPMI clipped at 0, adoption 3/7 |
| `tests/test_esi.py` | 5 | percentile rank 0.0/0.5/1.0, unit range, **missing component renormalisation** (G=0.9, C=0.3, T missing → 0.6 not 0.4), `components_available` counts, rank ordering |
| `tests/test_dashboard.py` | 2 | all three pages render without exception; the KPI tiles equal the real pipeline counts (775,275 / 3,608,325) |

Expected values are computed by hand in comments above each assertion, so a formula change that breaks a
definition fails loudly instead of drifting.

---

## 7. End-to-end acceptance

A clean run of `./scripts/run_pipeline.sh` completes all six stages and reproduces every output:

```
== 1/6 dataset ==      checksum verified
== 2/6 validate ==     measured profile printed
== 3/6 HDFS ingest ==  row count reconciled
== 4/6 Pig ETL ==      775,275 postings · 3,608,325 job-skill rows
== 5/6 Hive warehouse ==  external tables + CTAS + 25 queries
== 6/6 Spark ==        trends · pairs · adoption · ESI · model
real 4m46.096s
```

`skill_demand` and `outputs/spark/*.csv` were regenerated inside this run (verified by file timestamps),
so Spark consumed fresh Hive output rather than stale data.

---

## 8. What was not measured

- **Distributed speedup** — impossible with one node; not claimed.
- **Per-stage CPU/memory profiling** — not instrumented.
- **Skill-extraction precision/recall** — not applicable: the dataset supplies structured skill labels,
  so there is no extraction step to evaluate.
- **Independent validation corpus** — running the same ESI on a second dataset would strengthen the
  robustness claim and was out of scope for the available time.
