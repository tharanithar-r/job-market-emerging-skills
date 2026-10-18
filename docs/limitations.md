# Limitations

Every limitation below is either measured in this project or inherited from the data and stated as such.
None of them are hypothetical.

## 1. The data describes 2023, not today

`job_posted_date` spans 2023-01-01 → 2023-12-31. Any statement of the form "X is emerging" means
"X was emerging during 2023". The analysis was run in 2026, so the findings are historical.

**What this does not invalidate:** the pipeline, the index definition, the validation method and the
tests are all independent of the year and would run unchanged on newer postings.

## 2. Advertised roles are not the labour market

The corpus is online data/IT job advertisements. Blue-collar roles, informal work, internal promotions
and jobs never advertised are all absent. Coverage is also skewed toward employers who post on the
aggregators that fed this dataset.

Every use of the phrase "job market" in this project means "advertised data/IT roles in 2023".

## 3. Occupational granularity is coarse

`job_title_short` has 10 values; splitting seniority leaves **7 occupations**. Consequences:

- Occupational adoption saturates: nearly every skill appears in all 7, so `adoption_share` = 1.0 for
  almost everything. Measured evidence: every top-15 skill scored exactly 0.4054 on that component in the
  four-component index.
- This is why adoption was removed from the ESI rather than left in as decoration.
- Co-occurrence patterns *within* an occupation cannot be studied, because occupations are too broad.

## 4. Salary coverage is 2.8%

Only 21,598 postings disclose an annual salary (after cleaning to 15,000–960,000). Consequences:

- No salary component in the index.
- The regression is trained on 17,823 rows and tested on 3,775 — adequate for a course-scale model, not
  for a production salary predictor.
- **Disclosure is non-random.** Pay-transparency jurisdictions, large employers and senior roles disclose
  more often. Salary medians describe salary-disclosing postings.
- The model's R² of 0.264 means ~74% of variance is unexplained. Using it to predict an individual
  posting's salary would be irresponsible.

## 5. Geographic artifact

Sudan appears with **21,781 postings**, more than the Netherlands or Italy. This is implausible for the
underlying labour market and is treated as a geo-tagging or scraping artifact. Country-level conclusions
must exclude or flag it; the audit records it rather than quietly cleaning it away.

## 6. The skill vocabulary is closed

252 curated skill tags. A skill that is genuinely new but not in this vocabulary is invisible, and "not
tagged" is indistinguishable from "no longer requested". This bounds what the ESI can discover: it ranks
*known* skills by their dynamics, it does not discover unknown skills.

The tags are also provider-assigned. We do not control how they were produced, so the skill layer
inherits an unknown extraction method — recorded as an unknown rather than described as if we knew it.

## 7. Duplicate detection is a lower bound

The dataset has no `job_id` or `job_link`. Deduplication uses the composite key
`(job_title, company_name, job_location, job_posted_date)`, which removed 10,465 rows (1.33%). A repost
that changed its title or date would survive. The measured duplicate rate is therefore a **lower bound**,
and residual duplication inflates counts by an unknown amount.

## 8. Volume does not justify a cluster

231 MB / 785,741 source rows. A single tool would not struggle with the source file. The big-data case
rests on two things: the need for **both batch and query-style access** (Pig ETL and Hive SQL over the
same data), and the **derived expansion to 3,608,325 job-skill rows** and ~8M raw pair instances.

Anyone arguing this needed Hadoop for volume would be wrong. It is a genuine distributed-pipeline
exercise at a deliberately modest scale.

## 9. Single node, no speedup claim

HDFS replication is 1 and both MapReduce and Spark run in local mode. No distributed-speedup measurement
is reported because none would be meaningful. Execution-mode details are in `docs/architecture.md` §6.

## 10. Co-occurrence is not causation

Two skills appearing together means employers ask for both. It says nothing about one driving demand for
the other. All pair findings are associative.

## 11. ESI is a rank construct, not a physical quantity

The score is an average of percentile ranks. It has no units and its magnitude is not comparable across
runs with different populations. Only ordering and component values are meaningful. The weights are a
design choice; the sensitivity analysis (top-10 overlap 0.6–1.0) shows the ranking is robust but not
weight-independent.

## 12. Known cosmetic issue

During Hive query execution one `MapredLocalTask` error appears once in the log while every result set
still returns complete and correct values (verified by comparing Hive's counts against Pig's own stage
counters). It is recorded here rather than omitted; it does not affect any output used in the analysis.
