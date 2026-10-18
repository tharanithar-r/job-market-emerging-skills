# Decision Log

Every locked decision, the alternative rejected, and why. This is the document to have open during Q&A.

## Dataset

| Decision | Chosen | Rejected alternatives |
|---|---|---|
| Primary dataset | `lukebarousse/data_jobs` — 785,741 rows, 12 clean monthly periods, 85% structured skills, Apache-2.0 | **`asaniczka/1-3m-linkedin`** (1.35M rows): the "2024" label is misleading — `first_seen` spans **2024-01-12 → 2024-01-17**, six days, so no temporal analysis is possible. **`edwarddgao/open-apply-jobs`** (61.5M rows, MIT): real scale and a genuine 4-month daily panel, but 99.3% of postings recur the next day (it is a snapshot of *open* roles, not a stream of new ones) and skills must be extracted from HTML. **`zalizedata`**: 394k rows but a single-day scrape and CC BY-NC. **`techmap`/MCM**: CC BY-**ND** (NoDerivatives), which a derived analytical project cannot honour. **Zenodo 2024–2026 panel**: real panel, CC BY 4.0, but only 115k rows and tech-only |
## Storage and ETL

| Decision | Chosen | Rejected alternatives |
|---|---|---|
| Dedup key | Composite `(job_title, company_name, job_location, job_posted_date)` at **date** granularity | Content hash (merges genuinely distinct same-title roles, and the brief forbids merging distinct jobs); no dedup (inflates every count). Measured: 10,465 duplicates, 1.33% |
| CSV parsing | `CSVExcelStorage` from `piggybank.jar` | `PigStorage(',')` splits inside quoted fields — `job_title` contains commas; `STRSPLIT` with a comma literal silently fails to split here; `TOKENIZE` was used for skills instead of `STRSPLIT` for exactly that reason |
| Storage format | **Parquet** for cleaned and warehouse data | TSV: 11 lines of the source contain tabs, so a delimiter-based format is fragile; Parquet also gives Hive and Spark a shared, typed contract |
| Pig ↔ Hive | Parquet on HDFS + Hive **external** tables | HCatalog/`HCatStorer`: Pig 0.18 targets Hive 3, this box runs Hive 4.2.1 |
| Spark ↔ Hive | Spark reads Parquet paths; **no shared metastore** | Sharing `~/hive_data/metastore_db` fails: Spark 4.2 bundles Hive 2.3.10 with Derby 10.16.1.1, Hive 4.2.1 uses Derby 10.17.1.0 |
| Partitioning | **None** — `period` is a column, not a partition key | Pig cannot write Hive-style partitions without HCatalog, and 775k rows across 12 periods gains nothing from pruning |

## Analysis

| Decision | Chosen | Rejected alternatives |
|---|---|---|
| Period | **Monthly**, 12 periods | Weekly (52 periods): noisier at this row count, and the ESI's trend component is more stable with monthly aggregates |
| Trend method | **OLS slope** | Share of consecutive increases (ignores magnitude, only 12 discrete values); Kleinberg burst detection (needs parameter tuning that 12 points cannot justify) |
| Co-occurrence | **Jaccard *and* PPMI**, both reported | Jaccard alone is inflated by ubiquitous skills; PPMI alone is biased toward rare pairs. They disagree on purpose |
| Support floor | Skills ≥ 50 postings, pairs ≥ 50 postings (223 of 252 skills retained) | No floor: the pair table fills with one-off coincidences |
| Normalisation | **Percentile rank** | Min–max: a single outlier compresses every other skill into a narrow band |
| ESI components | **3 — G, T, C at 0.333** | 5 components: salary covers 2.8% so an S component is null/noise; adoption saturates at 1.0 with 7 occupations (measured: every top-15 skill scored exactly 0.4054). Both are computed and reported separately |
| ML split | **Time-based** (train Jan–Sep, test Oct–Dec) | Random split leaks same-period information and does not answer "generalise to later postings" |
| ML features | Occupation, seniority, country, schedule type, 3 benefit flags, top-10 skill indicators | Using the description text would need the raw text we deliberately do not redistribute, and would make the model far harder to explain |


## Things we tried that did not work

| Attempt | Outcome |
|---|---|
| `PigStorage(',')` on the raw CSV | Fields misaligned on quoted commas |
| `STRSPLIT(skill_csv, ',')` in Pig | Silently returned one field; `TOKENIZE(s, ',')` worked and preserved `"power bi"` |
| `REGEX_EXTRACT_ALL` with `\x27` for apostrophes | Returned empty; a quote-free character-class regex also failed. Solved by bracket-stripping + `SUBSTRING` |
| `ParquetStorer` with default Pig classpath | `Cannot instantiate class ... ParquetStorer` — needed `parquet-pig-bundle-1.9.0.jar` |
| `beeline -f script.sql` | Echoed every statement **without executing it**; `-e` executes |
| Hive on the default engine | `NoClassDefFoundError: org/apache/tez/...` — Tez not installed, forced `hive.execution.engine=mr` |
| Sharing the Derby metastore between Hive and Spark | `Another instance of Derby may have already booted` — abandoned by design |
| `OneHotEncoder` on `job_schedule_type` | `Cannot have an empty string for name` — 16 postings had an empty string category; relabelled `unknown` |
| Passing flags straight to `VectorAssembler` | `Data type string ... is not supported` — Pig emits `'true'/'false'`, converted to 1.0/0.0 |
| `df.write.csv("outputs/...")` | Files landed in **HDFS**, because the default filesystem is HDFS; forced an absolute `file://` URI |
