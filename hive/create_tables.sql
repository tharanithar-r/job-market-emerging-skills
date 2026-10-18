-- Phase 4a: warehouse tables.
-- Session settings (engine, local MR, scratch dirs) are prepended by scripts/run_hive.sh —
-- Hive does not inherit those from JVM -D flags, they must be SET in-session.

-- External tables point at the Parquet Pig wrote (name-matched by Parquet column names).
-- skill_demand is computed BY HIVE, so Hive does real analytical work, not just metadata.
CREATE DATABASE IF NOT EXISTS jobmarket;
USE jobmarket;

DROP TABLE IF EXISTS postings;
CREATE EXTERNAL TABLE postings (
  job_key            STRING,
  job_title          STRING,
  job_title_short    STRING,
  occupation         STRING,
  seniority          STRING,
  company_name       STRING,
  job_location       STRING,
  job_country        STRING,
  job_schedule_type  STRING,
  work_from_home     STRING,
  no_degree_mention  STRING,
  health_insurance   STRING,
  job_via            STRING,
  search_location    STRING,
  posted_date        STRING,
  period             STRING,
  salary_rate        STRING,
  salary_year_avg    STRING,
  salary_hour_avg    STRING,
  job_skills         STRING
)
STORED AS PARQUET
LOCATION '/jobmarket/cleaned/postings'
TBLPROPERTIES ('external.table.purge' = 'false');

DROP TABLE IF EXISTS job_skills;
CREATE EXTERNAL TABLE job_skills (
  job_key     STRING,
  period      STRING,
  occupation  STRING,
  seniority   STRING,
  job_country STRING,
  skill_norm  STRING
)
STORED AS PARQUET
LOCATION '/jobmarket/cleaned/job_skills'
TBLPROPERTIES ('external.table.purge' = 'false');

DROP TABLE IF EXISTS pipeline_quality;
CREATE EXTERNAL TABLE pipeline_quality (
  stage   STRING,
  records BIGINT
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '\t'
LOCATION '/jobmarket/warehouse/pipeline_quality'
TBLPROPERTIES ('external.table.purge' = 'false',
               'mapreduce.input.fileinputformat.input.dir.recursive' = 'true');

-- Hive-computed reporting table (used later to cross-check the Spark output).
DROP TABLE IF EXISTS skill_demand;
CREATE TABLE skill_demand
STORED AS PARQUET
AS
WITH totals AS (
  SELECT period, COUNT(*) AS total_postings FROM postings GROUP BY period
),
counts AS (
  SELECT period, skill_norm, COUNT(DISTINCT job_key) AS posting_count
  FROM job_skills GROUP BY period, skill_norm
)
SELECT c.period,
       c.skill_norm,
       c.posting_count,
       t.total_postings,
       c.posting_count / t.total_postings AS demand_share
FROM counts c JOIN totals t ON c.period = t.period;
