-- Phase 4c: SQL reporting. These are Hive-side cross-checks for the Spark results.
USE jobmarket;

-- Top skills in the final period by demand share
SELECT skill_norm, posting_count, total_postings,
       ROUND(demand_share * 100, 2) AS pct_of_postings
FROM skill_demand
WHERE period = '2023-12'
ORDER BY posting_count DESC
LIMIT 15;

-- Demand-share trend for a readable subset
SELECT period, skill_norm, ROUND(demand_share * 100, 2) AS pct
FROM skill_demand
WHERE skill_norm IN ('python', 'sql', 'spark', 'airflow', 'databricks', 'power bi')
ORDER BY skill_norm, period;

-- Occupation-level demand (seniority already split out by Pig)
SELECT occupation, COUNT(*) AS postings
FROM postings
GROUP BY occupation
ORDER BY postings DESC;

-- Salary summary per occupation, with sample counts (only n >= 100 reported)
SELECT occupation,
       COUNT(*) AS n,
       ROUND(AVG(CAST(salary_year_avg AS DOUBLE))) AS mean_salary,
       ROUND(PERCENTILE_APPROX(CAST(salary_year_avg AS DOUBLE), 0.5)) AS median_salary
FROM postings
WHERE salary_year_avg IS NOT NULL AND salary_year_avg != ''
  AND salary_rate = 'year'
GROUP BY occupation
HAVING COUNT(*) >= 100
ORDER BY n DESC;
