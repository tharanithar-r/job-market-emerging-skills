-- Phase 3a: clean the raw postings.
-- Input : /jobmarket/raw/postings/data_jobs.csv  (quoted CSV)
-- Output: /jobmarket/cleaned/postings            (Parquet) + pipeline_quality counts
%default RAW     /jobmarket/raw/postings/data_jobs.csv
%default CLEAN   /jobmarket/cleaned/postings
%default QUALITY /jobmarket/warehouse/pipeline_quality/clean_postings

-- 1. Load. CSVExcelStorage handles quoted commas (needs piggybank on PIG_CLASSPATH).
raw = LOAD '$RAW'
      USING org.apache.pig.piggybank.storage.CSVExcelStorage(',', 'NO_MULTILINE', 'UNIX', 'SKIP_INPUT_HEADER')
      AS (job_title_short:chararray, job_title:chararray, job_location:chararray, job_via:chararray,
          job_schedule_type:chararray, job_work_from_home:chararray, search_location:chararray,
          job_posted_date:chararray, job_no_degree_mention:chararray, job_health_insurance:chararray,
          job_country:chararray, salary_rate:chararray, salary_year_avg:chararray, salary_hour_avg:chararray,
          company_name:chararray, job_skills:chararray, job_type_skills:chararray);

-- 2. Reject unusable records: no title or no date means the row cannot be analysed.
valid = FILTER raw BY job_title IS NOT NULL AND TRIM(job_title) != ''
                   AND job_posted_date IS NOT NULL AND TRIM(job_posted_date) != '';

-- 3. Derive fields. Salary stays chararray: casting in Pig throws on junk values,
--    Spark handles null/'' cleanly later.
proj = FOREACH valid GENERATE
    LOWER(CONCAT(CONCAT(CONCAT(CONCAT(
        TRIM(job_title), '|'), TRIM(company_name)), '|'),
        CONCAT(CONCAT(TRIM(job_location), '|'), SUBSTRING(job_posted_date, 0, 10)))) AS job_key,
    TRIM(job_title)                 AS job_title,
    TRIM(job_title_short)           AS job_title_short,
    -- "Senior Data Engineer" -> occupation "Data Engineer", seniority "Senior"
    (TRIM(job_title_short) MATCHES '^Senior .*'
        ? TRIM(SUBSTRING(job_title_short, 7, 64)) : TRIM(job_title_short)) AS occupation,
    (TRIM(job_title_short) MATCHES '^Senior .*' ? 'Senior' : 'Mid')       AS seniority,
    TRIM(company_name)              AS company_name,
    TRIM(job_location)              AS job_location,
    TRIM(job_country)               AS job_country,
    TRIM(job_schedule_type)         AS job_schedule_type,
    LOWER(TRIM(job_work_from_home)) AS work_from_home,
    LOWER(TRIM(job_no_degree_mention)) AS no_degree_mention,
    LOWER(TRIM(job_health_insurance))  AS health_insurance,
    TRIM(job_via)                   AS job_via,
    TRIM(search_location)           AS search_location,
    SUBSTRING(job_posted_date, 0, 10) AS posted_date,
    SUBSTRING(job_posted_date, 0, 7)  AS period,
    LOWER(TRIM(salary_rate))        AS salary_rate,
    TRIM(salary_year_avg)           AS salary_year_avg,
    TRIM(salary_hour_avg)           AS salary_hour_avg,
    TRIM(job_skills)                AS job_skills;

-- 4. Deduplicate on the composite key (D2). Keep one arbitrary-but-stable row per key.
grouped  = GROUP proj BY job_key;
deduped  = FOREACH grouped {
    ordered = ORDER proj BY posted_date, job_title;
    first   = LIMIT ordered 1;
    GENERATE FLATTEN(first);
};

-- 5. Re-project with clean names. FLATTEN(first) prefixes every field with "first::",
--    and Hive matches Parquet columns BY NAME, so un-renamed fields would all read NULL.
final = FOREACH deduped GENERATE
    first::job_key            AS job_key,
    first::job_title          AS job_title,
    first::job_title_short    AS job_title_short,
    first::occupation         AS occupation,
    first::seniority          AS seniority,
    first::company_name       AS company_name,
    first::job_location       AS job_location,
    first::job_country        AS job_country,
    first::job_schedule_type  AS job_schedule_type,
    first::work_from_home     AS work_from_home,
    first::no_degree_mention  AS no_degree_mention,
    first::health_insurance   AS health_insurance,
    first::job_via            AS job_via,
    first::search_location    AS search_location,
    first::posted_date        AS posted_date,
    first::period             AS period,
    first::salary_rate        AS salary_rate,
    first::salary_year_avg    AS salary_year_avg,
    first::salary_hour_avg    AS salary_hour_avg,
    first::job_skills         AS job_skills;

-- 6. Store.
STORE final INTO '$CLEAN' USING org.apache.pig.builtin.ParquetStorer();

-- 6. Row accounting, so every stage reconciles.
qc_raw   = FOREACH (GROUP raw ALL)     GENERATE 'raw_loaded'   AS stage, COUNT(raw)     AS records;
qc_valid = FOREACH (GROUP valid ALL)   GENERATE 'after_filter' AS stage, COUNT(valid)   AS records;
qc_dedup = FOREACH (GROUP deduped ALL) GENERATE 'after_dedup'  AS stage, COUNT(deduped) AS records;
STORE (UNION qc_raw, qc_valid, qc_dedup) INTO '$QUALITY' USING PigStorage('\t');
