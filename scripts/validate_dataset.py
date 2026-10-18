"""Measure the dataset. One pass, stdlib only.

Usage: python3 scripts/validate_dataset.py [path/to/data_jobs.csv]
"""
import csv
import ast
import sys
from collections import Counter

# ponytail: composite key per decision D2 (no job_id/job_link exists in this dataset)
KEY = ("job_title", "company_name", "job_location", "job_posted_date")


def measure(path):
    rows = 0
    empty = Counter()
    months = Counter()
    country = Counter()
    occupation = Counter()
    pay_rate = Counter()
    keys = Counter()
    skill_counter = Counter()
    skill_parse_fail = 0
    with_skills = with_salary_yr = with_salary_hr = 0
    date_min = None
    date_max = None

    with open(path, newline="", encoding="utf-8", errors="replace") as f:
        reader = csv.DictReader(f)
        cols = reader.fieldnames
        for row in reader:
            rows += 1
            for c in cols:
                if not (row[c] or "").strip():
                    empty[c] += 1

            posted = (row["job_posted_date"] or "").strip()
            if posted:
                months[posted[:7]] += 1
                date_min = posted if date_min is None or posted < date_min else date_min
                date_max = posted if date_max is None or posted > date_max else date_max

            country[row["job_country"]] += 1
            occupation[row["job_title_short"]] += 1
            pay_rate[row["salary_rate"]] += 1
            with_skills += bool((row["job_skills"] or "").strip())
            raw_skills = (row["job_skills"] or "").strip()
            if raw_skills:
                try:
                    parsed = ast.literal_eval(raw_skills)
                    if isinstance(parsed, (list, tuple)):
                        skill_counter.update(str(s).strip().lower() for s in parsed if str(s).strip())
                except (ValueError, SyntaxError):
                    skill_parse_fail += 1
            with_salary_yr += bool((row["salary_year_avg"] or "").strip())
            with_salary_hr += bool((row["salary_hour_avg"] or "").strip())

            keys["|".join((row[k] or "").strip().lower() for k in KEY)] += 1

    unique = len(keys)
    dup_rows = rows - unique
    assert rows == sum(keys.values()), "row accounting broken"
    assert 0 <= dup_rows < rows, "duplicate accounting broken"

    print(f"file            {path}")
    print(f"rows            {rows:,}")
    print(f"columns         {len(cols)}")
    print(f"date range      {date_min} -> {date_max}")
    print(f"months          {len(months)}")
    print(f"rows/month      {min(months.values()):,} .. {max(months.values()):,}")
    print()
    print(f"unique (D2 key) {unique:,}")
    print(f"duplicate rows  {dup_rows:,}  ({100 * dup_rows / rows:.2f}%)")
    print(f"max key repeats {max(keys.values()):,}")
    print()
    print(f"with skills     {with_skills:,}  ({100 * with_skills / rows:.1f}%)")
    print(f"salary_year_avg {with_salary_yr:,}  ({100 * with_salary_yr / rows:.1f}%)")
    print(f"salary_hour_avg {with_salary_hr:,}  ({100 * with_salary_hr / rows:.1f}%)")
    print()
    print("empty counts (top 10):")
    for c, n in empty.most_common(10):
        print(f"  {c:<24} {n:>8,}  ({100 * n / rows:.1f}%)")
    print()
    print()
    print(f"distinct skills        {len(skill_counter):,}")
    print(f"total skill mentions   {sum(skill_counter.values()):,}")
    print(f"skill list parse fails {skill_parse_fail:,}")
    print(f"skills >= 50 postings  {sum(1 for n in skill_counter.values() if n >= 50):,}")
    print(f"skills >= 10 postings  {sum(1 for n in skill_counter.values() if n >= 10):,}")
    print(f"skills <  10 postings  {sum(1 for n in skill_counter.values() if n < 10):,}")
    print("top 20 skills:")
    for v, n in skill_counter.most_common(20):
        print(f"  {v:<28} {n:>8,}")
    print()
    print("rows per month:")
    for v, n in sorted(months.items()):
        print(f"  {v}  {n:>8,}")
    print()
    print("occupation (job_title_short):")
    for v, n in occupation.most_common():
        print(f"  {v:<28} {n:>8,}")
    print()
    print("countries: top 10 of", len(country))
    for v, n in country.most_common(10):
        print(f"  {v:<28} {n:>8,}")
    print()
    print("salary_rate values:")
    for v, n in pay_rate.most_common():
        print(f"  {str(v):<28} {n:>8,}")


if __name__ == "__main__":
    measure(sys.argv[1] if len(sys.argv) > 1 else "data/raw/data_jobs.csv")
