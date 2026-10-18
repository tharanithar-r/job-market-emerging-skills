# Architecture

## 1. End-to-end flow

```
┌────────────────────┐
│ lukebarousse/      │  HTTPS, no token
│ data_jobs          │  785,741 rows · 231 MB · Apache-2.0
└─────────┬──────────┘
          │ scripts/download_data.sh  (+ SHA-256 verification)
          ▼
┌──────────────────────────────────────────────────────────────────┐
│ HDFS  /jobmarket/                                                │
│   raw/postings/data_jobs.csv        original file, unmodified    │
│   cleaned/postings/                 Parquet, 775,275 rows        │
│   cleaned/job_skills/               Parquet, 3,608,325 rows      │
│   warehouse/pipeline_quality/       stage counters (TSV)         │
│   warehouse/skill_trends/           Parquet                      │
│   warehouse/skill_pairs/            Parquet                      │
│   warehouse/occupational_adoption/  Parquet                      │
│   warehouse/esi_scores/             Parquet                      │
└───────┬──────────────────────────┬───────────────────────────────┘
        │                          │
        ▼                          ▼
┌───────────────────┐    ┌──────────────────────────┐
│ Apache Pig 0.18   │    │ Apache Hive 4.2.1        │
│ clean_postings    │───▶│ external tables over the │
│ build_job_skills  │    │ same Parquet + CTAS      │
└───────────────────┘    │ skill_demand             │
                         │ validation SQL           │
                         └───────────┬──────────────┘
                                     │ reading Hive's output
                                     ▼
                         ┌──────────────────────────┐
                         │ Apache Spark 4.2 + MLlib │
                         │ skill_trends             │
                         │ skill_cooccurrence       │
                         │ occupational_adoption    │
                         │ emerging_skill_index     │
                         │ salary_model             │
                         └───────────┬──────────────┘
                                     │ CSV exports
                                     ▼
                         ┌──────────────────────────┐
                         │ Streamlit dashboard      │
                         │ 3 pages, reads outputs   │
                         └──────────────────────────┘
```

## 2. Why the handoffs are file-based

Hive and Spark **do not share a metastore** here. Spark 4.2 bundles Hive 2.3.10 with Derby
`10.16.1.1`; the installed Hive 4.2.1 uses Derby `10.17.1.0`. Pointing both at
`~/hive_data/metastore_db` fails at startup. Rather than force a metastore upgrade, the contract between
the engines is **Parquet on HDFS**:

- Pig writes Parquet to fixed paths.
- Hive declares **external tables** over exactly those paths (`LOCATION`), so it never copies or owns the data.
- Spark reads those same paths directly — and also reads Hive's `skill_demand` output.

The consequence is deliberate: any engine can be replaced without rewriting the others, and there is one
copy of the data rather than one per engine.

## 3. Stage responsibilities

| Stage | Input | Output | Transformation it owns |
|---|---|---|---|
| Ingestion | HTTPS CSV | HDFS raw | checksum verification, row reconciliation |
| **Pig** | raw CSV | cleaned Parquet | quoted-CSV parsing, type/date casting, validation filter, composite-key dedup, seniority split, skill explosion |
| **Hive** | cleaned Parquet | external tables + `skill_demand` | column contract, partition-free SQL validation, demand aggregation |
| **Spark** | Parquet | warehouse tables | temporal features, pair metrics, percentile normalisation, MLlib |
| Dashboard | CSV exports | rendered pages | presentation only |

## 4. Data model

**`cleaned/postings`** — one row per deduplicated posting (20 columns):
`job_key, job_title, job_title_short, occupation, seniority, company_name, job_location, job_country,
job_schedule_type, work_from_home, no_degree_mention, health_insurance, job_via, search_location,
posted_date, period, salary_rate, salary_year_avg, salary_hour_avg, job_skills`

**`cleaned/job_skills`** — one row per (posting, skill):
`job_key, period, occupation, seniority, job_country, skill_norm`

**Derived tables** — `skill_trends` (period × skill demand features), `skill_pairs` (period × pair with
Jaccard and PPMI), `occupational_adoption`, `esi_scores` (one row per scored skill).

## 5. Row accounting

Every stage records what it received and emitted, so nothing can be dropped silently:

```
raw_loaded            785,741
after_filter          785,740   (−1 · no job_title)
after_dedup           775,275   (−10,465 · same-day duplicates)
postings_with_skills  659,556
job_skill_rows      3,608,325
```

Hive independently re-derives the first three from the Parquet and matches them.


