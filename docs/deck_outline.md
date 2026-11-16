# Solution Deck Outline (10 slides) / 方案 Deck 大纲（10 页）

> Slide copy for the official deck: problem, approach, architecture, results, Responsible AI, next steps.
> Every number below comes from the status table of the last run with the local model on. Re-run and re-check before presenting.
> Architecture and operations slides can reuse the team's existing architecture material; this file covers
> the story, the engine and the results.

Judging weights to keep in mind: Business Impact 30 · Technical 25 · Innovation 20 · Responsible AI 15 · Presentation 10.

---

## Slide 1 — Title
- **Migration Factory: legacy T-SQL and SAS to PySpark, with evidence for every job**
- Challenge 03 · Agentic data platform modernisation · Team name · members
- One line: *AI proposes, reconciliation decides, a human signs off.*

## Slide 2 — The problem (Business Impact)
- Client: ~1,200 legacy ETL jobs and 400+ SAS programs; undocumented logic, unknown lineage, 4-6 month window.
- Hard requirement: every migrated pipeline must reconcile with legacy output, record for record.
- The real cost is not translation, it is **trust**: an unverified translation still needs a full manual check.
- So the useful question is: *for how many jobs can a reviewer skip the manual check, and why?*

## Slide 3 — Our answer in one picture
- Every job ends in one of three labels, each with evidence attached:
  - **Verified** — translated by fixed rules, matches legacy on sample, adversarial and empty data.
  - **Tested** — translated by an LLM, matches legacy on the same data.
  - **Needs review** — everything else, with the reason and a minimal failing example.
- Reviewers spend time only where the label says they must.

## Slide 4 — How it works (Technical approach)
- **Rule path**: `sqlglot` parses T-SQL → small typed IR → deterministic PySpark generator. Same input, same code, no model call.
- **LLM path**: only for what the rules do not cover (MERGE, stored procedures, SAS). At most 3 repairs.
- **Reconciliation**: row count → column totals → row by row; DECIMAL exact, FLOAT within 1e-9.
- **Three datasets per job**: real sample, synthetic adversarial rows, empty tables.

## Slide 5 — What makes it different (Innovation)
- **SQL Server semantics are encoded as rules**, each confirmed on a real SQL Server: integer division, `AVG(int)`, decimal truncation, case-insensitive comparison, `ISNULL` typing, trailing spaces.
- **Adversarial data is built from the job itself**: the constants a job compares against, plus both neighbours.
- **Failing inputs are shrunk** to the few rows that still fail, and only that goes back to the model.
- **The label is earned, not declared**: the engine decides the path and the level; metadata cannot override it.

## Slide 6 — Architecture
- Reuse the one-page architecture diagram (`docs/architecture_diagram.svg`).
- Flow: legacy code → engine (rule path / LLM path) → reconciliation → status table → human sign-off → Bronze / Silver / Gold.
- CI runs the reconciliation gate on every pull request.

## Slide 7 — Results
- 13 jobs: 10 T-SQL + 3 SAS.
- **7 Verified** (all by the rule path, zero model calls) · **1 Tested** (J10, translated by the local model) · **5 Needs review**.
- Verified jobs cover: cleansing, aggregation, LEFT JOIN with zero-fill, ratios, averages, risk tiers, a data-quality check.
- Each one targets a known trap (e.g. J04 integer division, J05 `AVG` over integers, J01 case-insensitive text).
- Show the dashboard Overview page here.

## Slide 8 — What did not work, and why we show it (Responsible AI)
- J08 (MERGE / SCD2) and J09 (stored procedure, ranking): the small local model did not reach parity in 3 repairs. J09 still differs in 17 rows; J08's code does not run.
- S01-S03 (SAS): no SAS runtime, so there is no legacy output to compare against. They stay Needs review by design.
- "Verified" is evidence from testing on defined data, not a guarantee for all inputs.
- We would rather show five honest Needs review than five unverified "done".

## Slide 9 — Safeguards (Responsible AI)
- The model sees code, schemas and **synthetic rows only**; real values are blocked from prompts by code and by test.
- Local model: prompts do not leave the machine. Every call is logged.
- AI-written code is statically checked before it runs.
- **Human sign-off** is required for Gold, is tied to the SHA-256 of the reviewed code, and lapses if the code changes.
- Public dataset (freMTPL2), no personal data.

## Slide 10 — Measuring success and next steps
- **How to measure**: share of jobs auto-labelled Verified; reviewer hours per job by label; reconciliation defects found after sign-off (target: zero).
- **Next**: widen the rule path (window functions, UNION, MERGE); a stronger model for the LLM path; a SAS runtime or exported SAS outputs as the oracle; scheduling and SLA monitoring on Azure Databricks.
- Close: *the factory's output is not code, it is code plus the evidence that lets someone trust it.*

---

## Demo video (max 3 minutes) — suggested sequence
1. (20 s) Overview: 13 jobs, 7 / 1 / 5, progress bars, nothing signed off yet.
2. (35 s) Job detail J04: legacy SQL next to PySpark, "why the PySpark looks this way" (integer division), reconciliation verdict.
3. (30 s) Job detail J10, AI audit tab: the exact prompt and answer, then the reconciliation that made it Tested.
4. (30 s) Review & actions on J04: tick the box, sign off. Overview now shows 1 signed off; run the Gold deployment gate.
5. (25 s) Job detail J09: Needs review, 17 differing rows; sign-off is not offered.
6. (25 s) Try your own code: paste a query, see the rule path and the generated PySpark.
7. (15 s) Analytics and the CI run on GitHub.
