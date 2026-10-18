# Dataset Audit

> Every number below was **measured** on the downloaded file, not copied from the
> provider's page. Reproduce with `python3 scripts/validate_dataset.py`.

## 1. Provenance

| Item | Value |
|---|---|
| Dataset | `lukebarousse/data_jobs` ("Data Jobs") |
| Official source | https://huggingface.co/datasets/lukebarousse/data_jobs |
| Download method | `curl -L` from the HF `resolve` URL — no account or token required |
| Access date | 2026-10-17 |
| Licence | **Apache-2.0** (copy, modify, redistribute, commercial use permitted with attribution) |
| File | `data/raw/data_jobs.csv`, **231,152,089 bytes** (220 MiB), UTF-8 CSV with header row |
| SHA-256 | `635241ed09ccee18bdae1f83b45f26d6759e0aa2513c529f6190e9054062436c` |
| Structure | **raw**, one row per job posting. Not aggregated, not pre-filtered by the provider |

## 2. Scale and shape

| Metric | Value |
|---|---|
| Rows | **785,741** |
| Columns | **17** |
| Duplicates, full-timestamp key | **873 rows (0.11%)** → 784,868 unique |
| Duplicates, date-level key (what Pig applies) | **10,465 rows (1.33%)** → 775,276 unique |
| Max repeats of one key | 6 |

The composite key includes `job_posted_date`. Deduplicating at **date** granularity
(Pig truncates the timestamp to `YYYY-MM-DD`) collapses a further **9,592 same-day
repeats** — the same role re-listed within one day. The pipeline therefore keeps
**775,275 rows** = 785,741 − 1 rejected − 10,465 duplicates, which is exactly the
predicted 775,276 minus the single rejected record. Duplication is still low enough
that the key does not need revisiting.

## 3. Time coverage

| Item | Value |
|---|---|
| `job_posted_date` range | `2023-01-01 00:00:04` → `2023-12-31 23:59:58` |
| Distinct periods | **12 monthly periods**, no gaps |
| Rows per month | 52,104 – 91,822 |

| Month | Rows | Month | Rows |
|---|---:|---|---:|
| 2023-01 | 91,822 | 2023-07 | 63,777 |
| 2023-02 | 64,578 | 2023-08 | 75,162 |
| 2023-03 | 64,084 | 2023-09 | 62,359 |
| 2023-04 | 62,919 | 2023-10 | 66,611 |
| 2023-05 | 52,104 | 2023-11 | 64,450 |
| 2023-06 | 61,572 | 2023-12 | 56,303 |

**This is the decisive property of the dataset.** Every other candidate examined
had either a single-day scrape or daily snapshots with heavy re-observation
(99.3% day-over-day overlap on one alternative). Twelve clean periods is what
makes period-over-period demand-share analysis — and therefore the Emerging
Skill Index — measurable rather than decorative.

## 4. Skills

| Metric | Value |
|---|---|
| Postings with a non-empty `job_skills` | **668,704 (85.1%)** |
| Total skill mentions (raw file) | **3,660,283** |
| Job-skill rows after dedup (Pig output) | **3,608,325** |
| Average skills per posting (of those with skills) | **5.47** |
| Distinct skills | **252** |
| Skills in ≥ 50 postings | **223** |
| Skills in ≥ 10 postings | 240 |
| Skills in < 10 postings | 12 |
| Skill-list parse failures | **0** |

Format is a Python-list string, e.g. `"['sql', 'python', 'power bi']"` — parseable
deterministically, no NLP or LLM extraction required.

Top 20 skills by posting count:

| Skill | Postings | Skill | Postings |
|---|---:|---|---:|
| sql | 384,849 | java | 85,612 |
| python | 380,909 | sas | 83,404 |
| aws | 145,381 | hadoop | 64,842 |
| azure | 132,527 | scala | 56,886 |
| r | 130,892 | snowflake | 55,015 |
| tableau | 127,213 | databricks | 53,182 |
| excel | 127,018 | gcp | 50,727 |
| spark | 114,609 | git | 50,226 |
| power bi | 98,147 | kafka | 49,926 |
| | | airflow | 45,254 |
| | | oracle | 45,221 |

**Consequence for co-occurrence:** a 252-skill vocabulary caps the pair space at
`252 × 251 / 2 = 31,626` distinct pairs. That is small and cheap to aggregate, so
a minimum-support threshold of 50 (keeping 223 skills) is affordable.

## 5. Occupation, geography, company

| Field | Coverage | Values |
|---|---|---|
| `job_title_short` | **100%** (0 empty) | **10** |
| `job_country` | 99.99% (49 empty) | **160** |
| `company_name` | ~100% | — |
| `job_location` | 99.87% (1,045 empty) | free text |
| `job_schedule_type` | 98.4% (12,667 empty) | 47 |
| `job_work_from_home` / `job_no_degree_mention` / `job_health_insurance` | 100% | booleans |

| `job_title_short` | Rows |
|---|---:|
| Data Analyst | 196,075 |
| Data Engineer | 186,241 |
| Data Scientist | 172,286 |
| Business Analyst | 49,063 |
| Software Engineer | 44,929 |
| Senior Data Engineer | 44,563 |
| Senior Data Scientist | 36,957 |
| Senior Data Analyst | 29,216 |
| Machine Learning Engineer | 14,080 |
| Cloud Engineer | 12,331 |

Seniority is embedded in the title (`Senior …`), so it will be split into a
separate `seniority` column, leaving **7 core occupations + seniority**.

Top countries: United States 206,292 · India 51,088 · United Kingdom 40,375 ·
France 39,922 · Germany 27,694 · Spain 25,100 · Singapore 23,696 · **Sudan 21,781** ·
Netherlands 20,631 · Italy 17,013.

## 6. Missing values (important fields)

| Field | Missing | % |
|---|---:|---:|
| `salary_hour_avg` | 775,079 | 98.6% |
| `salary_year_avg` | 763,738 | **97.2%** |
| `salary_rate` | 752,674 | 95.8% |
| `job_skills` | 117,037 | 14.9% |
| `job_type_skills` | 117,037 | 14.9% |
| `job_schedule_type` | 12,667 | 1.6% |
| `job_location` | 1,045 | 0.1% |
| `job_country` | 49 | 0.0% |
| `job_title` | 1 | 0.0% |

## 7. Salary availability

| Field | Non-empty | % of rows |
|---|---:|---:|
| `salary_year_avg` | 22,003 | **2.8%** |
| `salary_hour_avg` | 10,662 | 1.4% |
| `salary_rate` (any) | 23,067 | 2.9% |

`salary_rate` distribution: `year` 22,004 · `hour` 10,662 · `month` 382 ·
`week` 10 · `day` 9.

**Salary is the weak dimension.** ~23,000 usable records is enough for descriptive
statistics and a small regression, but not for a salary component inside the ESI.
This is why salary is reported separately rather than scored (decision D7), and why
the index uses three components: growth, trend and co-occurrence.

## 8. Duplicate handling

The dataset has **no `job_id` and no `job_link`**, so no unique identifier exists.
A composite key is used (decision D2):

```
job_title | company_name | job_location | job_posted_date   (lowercased, trimmed)
```

Measured effect:
- full-timestamp key: **784,868 unique / 785,741 rows = 873 duplicates (0.11%)**
- date-level key (what the pipeline applies): **775,276 unique = 10,465 duplicates (1.33%)**

The `search_location` column does not produce the widespread re-capture that was
anticipated, but same-day re-listing does account for ~1.3% of rows. Deduplication is
therefore a correctness and hygiene step, not a major data reduction.

## 9. Does the dataset support the intended analyses?

| Feature | Supported | Notes |
|---|---|---|
| F1 Demand counts and shares | **Yes** | 12 periods × 252 skills; share denominators are clean |
| F2 Emerging-skill index | **Yes** | 3 scored components (growth, trend, co-occurrence); adoption saturates with 7 occupations |
| — Component B (trend consistency) | **Yes** | 12 points is enough for an OLS slope |
| F3 Skill co-occurrence | **Yes** | 252-skill vocabulary, ≤31,626 distinct pairs, 3.66M mentions |
| F4 Occupational adoption | **Yes** | 10 title categories → 7 occupations + seniority |
| F5 Salary analysis | **Partial** | 2.8% coverage; descriptive + small regression only |
| MLlib salary regression | **Partial** | 22,003 labelled rows; time-based split across 2023 |
| Geographic comparison | **Yes** | 160 countries, but see the Sudan artifact below |
| Industry analysis | **No** | No industry column exists — not fabricated |

Overall: **raw** data, suitable as the single primary source.

## 10. Limitations that constrain the conclusions

1. **2023 only.** The data is a 2023 cross-year, analysed in 2026. Findings describe
   the 2023 labour market, not the current one. The pipeline is the reusable asset.
2. **Coverage is not the labour market.** These are online data/IT postings
   aggregated from job boards. Blue-collar, informal, and non-advertised roles are
   absent. "Job market" throughout means "advertised data/IT roles".
3. **Scope bias.** Only 10 occupations, all data/IT. Occupational adoption is
   measured *between these roles*, not across the wider economy.
4. **Sudan artifact.** Sudan has 21,781 postings — more than the Netherlands or
   Italy. That is not a plausible labour market of that size and is almost
   certainly a scraping/geo-tagging artifact. Country-level analysis must exclude
   or flag it (decision D9).
5. **Salary self-selection.** The 2.8% with salary are not a random sample:
   disclosure correlates with employer type, seniority and jurisdiction. Any salary
   finding describes salary-disclosing postings only.
6. **No unique posting ID.** Deduplication rests on a composite key and cannot
   detect reposts that changed title or date. The measured 0.11% is therefore a
   *lower bound* on duplication.
7. **Skill vocabulary is closed.** 252 curated tags. Skills absent from this list
   cannot be detected, so "no longer emerging" and "never tagged" are
   indistinguishable.
8. **Skills are provider-assigned.** We do not control how the tags were produced,
   so the skills layer inherits an unknown extraction method.
9. **Volume.** 231 MB / 785,741 rows. This does not justify a cluster on volume
   grounds; the big-data case rests on batch + query-style access and the derived
   expansion to 3.66M job-skill rows.
