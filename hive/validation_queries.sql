-- Phase 4b: independent validation that Pig's cleaning actually worked.
USE jobmarket;

SELECT 'rows_postings' AS check_name, COUNT(*) AS value FROM postings;
SELECT 'distinct_periods' AS check_name, COUNT(DISTINCT period) AS value FROM postings;
SELECT 'duplicate_job_keys' AS check_name, COUNT(*) AS value
  FROM (SELECT job_key FROM postings GROUP BY job_key HAVING COUNT(*) > 1) d;
SELECT 'empty_job_title' AS check_name, COUNT(*) AS value
  FROM postings WHERE job_title IS NULL OR job_title = '';
SELECT 'job_skill_rows' AS check_name, COUNT(*) AS value FROM job_skills;
SELECT 'distinct_skills' AS check_name, COUNT(DISTINCT skill_norm) AS value FROM job_skills;
SELECT 'distinct_occupations' AS check_name, COUNT(DISTINCT occupation) AS value FROM postings;
SELECT 'postings_with_salary' AS check_name, COUNT(*) AS value
  FROM postings WHERE salary_year_avg IS NOT NULL AND salary_year_avg != '';
SELECT 'avg_skills_per_posting' AS check_name,
       ROUND(COUNT(*) / COUNT(DISTINCT job_key), 2) AS value FROM job_skills;

-- period coverage
SELECT period, COUNT(*) AS postings FROM postings GROUP BY period ORDER BY period;

-- stage accounting from Pig
SELECT * FROM pipeline_quality ORDER BY stage;
