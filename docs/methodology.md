# Methodology

This document defines every measure used in the pipeline, with the reasons behind each choice.
Implementation lives in `spark/*.py`; the corresponding pure-formula tests are in `tests/test_metrics.py`
and `tests/test_esi.py`.

Notation: *s* = skill, *t* = monthly period (12 periods, 2023-01 … 2023-12), *N_t* = postings in *t*.

---

## 1. Demand share and growth

```
D(s,t)  = N(s,t) / N_t
ΔD(s)   = D(s,t_last) − D(s,t_last−1)
```

`N(s,t)` counts **distinct postings** mentioning the skill in the period, not raw mentions — a posting
that lists `sql` twice still counts once (`countDistinct(job_key)`).

**Why share and not count:** raw counts rise when the corpus grows. Share normalises by period size, so
an increase means the skill takes a larger slice of the same market rather than the market being bigger.

**Relative growth is guarded.** `relative_growth = ΔD / D(t−1)` is emitted only when
`D(t−1) ≥ 0.001`. Below that floor, a skill seen for the first time would produce an unbounded value and
dominate any ranking. Skills crossing the floor are simply reported with a null and excluded from
growth-based ranking rather than being assigned infinity or an invented value.

---

## 2. Trend consistency (Component T)

An OLS slope of the skill's monthly demand shares:

```
slope = (n·Σxy − Σx·Σy) / (n·Σx² − (Σx)²),   x = period index 0..11, y = D(s,t)
```

Implemented as a single `groupBy(skill)` aggregation of `n, Σx, Σy, Σxy, Σx²` — no per-skill Python loop,
so it scales.

**Why OLS over the alternatives.** The share of consecutive increases is coarse with 12 points (it can
only take 12 values and ignores magnitude); Kleinberg burst detection is better suited to identifying
*when* a burst starts in a long stream and needs parameter tuning this corpus is too short to justify.
OLS is explainable in one sentence and behaves sensibly with 12 observations. The limitation is stated
plainly: a linear slope cannot represent a non-linear take-off, so a skill that inflects sharply upward
in the last three months ranks lower than its recent behaviour alone would suggest.

---

## 3. Co-occurrence (Component C input)

For a pair of skills *a,b* within period *t*:

```
c = postings in t containing both
a_count, b_count = postings in t containing each
N = all postings in t

Jaccard = c / (a_count + b_count − c)
PPMI    = max(0, log₂( (c/N) / ((a_count/N)·(b_count/N)) ))
        = max(0, log₂( c·N / (a_count·b_count) ))
```

**Both metrics, deliberately.** Jaccard measures set overlap and is inflated by skills that appear
everywhere — "communication" will pair with everything. PPMI asks whether the pair occurs *more often
than chance* and corrects for base rates, but it is biased toward rare pairs. Neither is sufficient
alone; reporting both lets a reader see a frequent-but-unsurprising pair (high Jaccard, low PPMI) next
to a rarer-but-surprising one (lower Jaccard, high PPMI).

**Support floor.** Skills must appear in ≥ 50 postings to enter the pair space, and pairs must occur in
≥ 50 postings to be stored. Without this, the pair table fills with one-off coincidences. The retained
vocabulary is 223 of 252 skills.

**Not causal.** Co-occurrence means employers ask for two things together. It does not mean one skill
causes demand for the other.

---

## 4. Occupational adoption (reported, not scored)

```
O(s,t) = distinct occupations requesting s in t / distinct occupations observed in t
```

The denominator is recomputed per period so that a change in *observed* occupations is not mistaken for
a skill spreading. This is the control the brief requires for coverage change.

**Why it is not in the index.** With 7 occupations (10 title categories minus seniority), almost every
skill appears in all of them, so `O` saturates at 1.0 and every skill receives the same value. A
component that is constant across the population contributes nothing to a ranking and would dilute the
weights of components that do discriminate. Measured evidence: in the four-component version of the
index, every skill in the top 15 scored exactly 0.4054 on this component.

It is still computed and shown, so a reader can verify the saturation claim.

---

## 5. Salary association (reported, not scored)

Coverage is **2.8%** of postings (21,598 rows after cleaning to annual salaries between 15,000 and
960,000). Two outputs:

1. **Descriptive** — median, p25 and p75 advertised annual salary per occupation and per skill, with the
   sample count attached to every row (n ≥ 100 only).
2. **Predictive** — Spark MLlib regression on `salary_year_avg`.

**Model design.** Features: occupation, seniority, country, schedule type, three boolean benefit flags,
and binary indicators for the ten most common skills. Categoricals are `StringIndexer` →
`OneHotEncoder`. Two estimators are compared against a mean-predictor baseline.

**Split is time-based**, not random: train on postings ≤ 2023-09-30, test on 2023-10-01 onwards
(17,823 / 3,775 rows). A random split would leak information from the same period and would not answer
the question "how well does this generalise to later postings".

**No causal claim.** Salary is disclosed by a non-random subset of employers (pay-transparency rules,
large firms, senior roles). Differences describe salary-disclosing postings, nothing more.

---

## 6. Emerging Skill Index

Every component is **percentile-ranked** across eligible skills before combining, so components measured
in incompatible units (share points, log ratios) become comparable ranks in [0, 1].

```
G = percentile rank of ΔD(s)                  (demand-share change, final period)
T = percentile rank of OLS slope              (trend over all 12 periods)
C = percentile rank of (mean PPMI at t_last − mean PPMI at t_first)
ESI = ⅓·G + ⅓·T + ⅓·C
```

A skill enters `C` through every pair it participates in, as *a* or as *b*.

**Why percentile rank rather than min–max.** Min–max is dominated by a single outlier — one skill with an
extreme slope compresses every other skill into a narrow band. Percentile ranks are robust to that and
are trivially interpretable: 1.0 means "highest-ranked skill on this component".

**Missing components.** If a skill has no pairs at the first period, `C` is null. The score is then the
weighted mean over the components that *are* available, and `components_available` records how many
contributed — so a skill scored on two components is visible as such rather than silently penalised or
silently inflated. In this run every scored skill has all three components.

**Weight sensitivity.** Equal weights are the transparent baseline, but they are a design choice, not a
constant. Four weightings are evaluated (equal, growth-heavy 0.5/0.3/0.2, trend-heavy 0.3/0.5/0.2,
co-occurrence-heavy 0.2/0.2/0.6) and the top-10 overlap with the baseline is reported. Measured: 1.0,
0.8, 0.8, 0.6 — the ranking is stable, and the co-occurrence dimension is the most independent.

**Validation against baselines.** The index is compared with a frequency-only ranking and a
growth-only ranking. Measured top-10 overlap: **1 of 10** against each; Spearman rank correlation across
all 223 scored skills: **0.333** (ESI vs frequency), **0.699** (ESI vs growth) and — the figure that
justifies the whole premise — **0.193** between frequency and growth themselves. That difference is the
project's result, not an implementation detail: popularity and growth are nearly unrelated, so a
frequency ranking cannot answer an emergence question.

---

## 7. What was deliberately not done

| Not done | Reason |
|---|---|
| Kleinberg burst detection | 12 periods is too short; parameter tuning would not be defensible |
| Salary inside the index | 2.8% coverage; the component would be null or noise for most skills |
| Occupational adoption inside the index | Saturates at 1.0 with 7 occupations |
| GraphX | The pair space is ≤ 31,626 edges — Spark aggregation is sufficient and simpler |
| TF-IDF / embedding skill extraction | The dataset already ships structured skill labels; re-extraction would add error, not information |
