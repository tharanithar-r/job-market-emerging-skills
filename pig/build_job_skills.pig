-- Phase 3b: explode each posting's skill list into one row per (job, skill).
-- Input : /jobmarket/cleaned/postings      (Parquet)
-- Output: /jobmarket/cleaned/job_skills     (Parquet)
%default CLEAN   /jobmarket/cleaned/postings
%default SKILLS  /jobmarket/cleaned/job_skills
%default QUALITY /jobmarket/warehouse/pipeline_quality/build_job_skills

postings = LOAD '$CLEAN' USING org.apache.pig.builtin.ParquetLoader()
    AS (job_key:chararray, job_title:chararray, job_title_short:chararray, occupation:chararray,
        seniority:chararray, company_name:chararray, job_location:chararray, job_country:chararray,
        job_schedule_type:chararray, work_from_home:chararray, no_degree_mention:chararray,
        health_insurance:chararray, job_via:chararray, search_location:chararray,
        posted_date:chararray, period:chararray, salary_rate:chararray,
        salary_year_avg:chararray, salary_hour_avg:chararray, job_skills:chararray);

with_skills = FILTER postings BY job_skills IS NOT NULL AND TRIM(job_skills) != '';

-- job_skills looks like "['sql', 'python', 'power bi']".
-- TOKENIZE with an explicit delimiter set splits on commas ONLY, so spaces inside a
-- skill survive and "power bi" stays one token. (STRSPLIT with a comma literal does
-- not split reliably here; verified.)
exploded = FOREACH with_skills GENERATE
    job_key, period, occupation, seniority, job_country,
    FLATTEN(TOKENIZE(job_skills, ',')) AS token;

-- A token is "['sql'" or " 'power bi']": drop brackets, trim, then remove the
-- surrounding single quotes by slicing off the first and last character.
trimmed = FOREACH exploded GENERATE
    job_key, period, occupation, seniority, job_country,
    TRIM(REPLACE(REPLACE(token, '\\[', ''), '\\]', '')) AS q;

named = FOREACH trimmed GENERATE
    job_key, period, occupation, seniority, job_country,
    (SIZE(q) > 2 ? SUBSTRING(q, 1, (int)(SIZE(q) - 1)) : '') AS skill_norm;

kept = FILTER named BY TRIM(skill_norm) != '';

final = FOREACH kept GENERATE
    job_key, period, occupation, seniority, job_country,
    TRIM(LOWER(skill_norm)) AS skill_norm;

STORE final INTO '$SKILLS' USING org.apache.pig.builtin.ParquetStorer();

-- Row accounting.
qc_in  = FOREACH (GROUP postings ALL)    GENERATE 'postings'             AS stage, COUNT(postings)    AS records;
qc_ws  = FOREACH (GROUP with_skills ALL) GENERATE 'postings_with_skills' AS stage, COUNT(with_skills) AS records;
qc_out = FOREACH (GROUP final ALL)       GENERATE 'job_skill_rows'       AS stage, COUNT(final)       AS records;
STORE (UNION qc_in, qc_ws, qc_out) INTO '$QUALITY' USING PigStorage('\t');
