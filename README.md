# Migration Factory — legacy T-SQL / SAS to PySpark, with proof

EXL × UCD Data & AI Hackathon 2026 · Challenge 03 (Agentic data platform modernisation).

Each legacy job is translated to PySpark and then **checked against the legacy system's own output**.
The result is one of three honest labels:

| Level | Meaning |
|---|---|
| **Verified** | Translated entirely by fixed rules, and matches legacy on real sample data, adversarial data and empty tables |
| **Tested** | Needed the LLM, and matches legacy on the same data |
| **Needs review** | Anything else: still differs after 3 AI repairs, legacy could not run, or any error |

Core idea: *AI proposes, the compiler disposes.* The LLM never has the last word.
Its code must pass the same reconciliation as rule-generated code.

## How it works

```
legacy.sql ──► parser ──► IR ──► code generator ──► PySpark ─┐
                 │ (outside IR scope, with a reason)          ├─► run on Spark ─► compare ─► level
                 └──────► LLM translator ──► PySpark ─────────┘        ▲             │ mismatch
                                ▲                              legacy output         ▼
                                └── shrunk counterexample (max 3 repairs) ◄── shrink to a few rows
```

- **Rule path**: `sqlglot` parses T-SQL; `engine/parser.py` builds a small language-neutral IR;
  `engine/codegen.py` emits PySpark by fixed rules. Same IR in, same code out.
- **SQL Server behaviours handled by the rules** (each was confirmed on a real SQL Server):
  integer division and `AVG(int)` drop the fraction; decimal division truncates instead of rounding;
  text comparison / `GROUP BY` ignore case and trailing spaces; `ISNULL` keeps its first argument's type;
  `LEN` ignores trailing spaces; `ROUND(x, n, 1)` truncates.
- **LLM path**: anything outside IR v1 (MERGE, window functions, stored procedures, all SAS).
- **Reconciliation** (`engine/recon/compare.py`): row count → column totals → row-by-row.
  Row order ignored; DECIMAL exact; FLOAT within 1e-9; no primary key → every distinct row must appear the same number of times.
- **Adversarial data** (`engine/recon/advdata.py`): NULLs, repeated keys, mixed case, trailing spaces,
  negatives, plus the constants each job compares against and their neighbours.
- **Shrinking** (`engine/recon/shrink.py`): a failing input is cut down to the fewest rows that still fail.

### IR v1 scope (the honest boundary)

Supported: one `SELECT` (CTEs and sub-queries in `FROM` allowed), `WHERE`, computed columns
(arithmetic, `CASE`, `COALESCE`/`ISNULL`/`NULLIF`, `CAST`, common text functions, comparisons, `IN`, `BETWEEN`, `LIKE`),
`INNER`/`LEFT JOIN`, `GROUP BY` with `SUM`/`COUNT`/`AVG`/`MIN`/`MAX`, `HAVING`.

Not supported (goes to the LLM path, with the reason recorded): `MERGE`, window functions, stored procedures and
control flow, temp tables, `UNION`, `DISTINCT`, `TOP`, sub-queries in conditions, `SELECT *`, all SAS.

## Setup (Windows cmd)

Needs Python 3.12 and a Java runtime (8, 11, 17 or 21).

```
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
copy .env.example .env
.venv\Scripts\python -m engine make-base --data-dir ..
```

`make-base` samples `freMTPL2freq.csv` / `freMTPL2sev.csv` (expected one folder up) into `datasets/base/`.

Optional services:

- **SQL Server** as the live legacy system. `.env.example` points at SQL Server LocalDB
  (`(localdb)\MSSQLLocalDB`); any SQL Server works by changing `MF_SQLSERVER_CONN`.
  Without it, the answer files committed under `jobs/*/golden/` are used.
- **Ollama** for the LLM path: `ollama pull qwen2.5-coder:3b`. Prompts stay on the machine.

## Run

```
.venv\Scripts\python -m engine run-all
.venv\Scripts\python -m engine run jobs\J04
.venv\Scripts\python -m engine run-all --legacy golden --llm off
.venv\Scripts\python -m pytest
```

`--legacy golden --llm off` is the CI mode: no SQL Server, no model, only committed answer files.
`--record-golden` saves SQL Server's outputs as answer files.

Drop any `.sql` file into `jobs/` and it runs through the same pipeline; what cannot be handled
becomes *Needs review* with a reason. Nothing crashes.

## Outputs (contract for the UI)

```
out/status.json              every job: level, path (ir | llm), reason, rules applied, repairs, datasets
out/<job>/ir.json            the IR (rule path only)
out/<job>/lineage.json       column-level lineage: "derived" (from IR) or "inferred, unverified" (LLM path)
out/<job>/generated.py       the PySpark code; AI-written code starts with an AI-GENERATED banner
out/<job>/recon_report.json  every attempt, per-dataset checks, sample differences, shrunk counterexample
out/<job>/ai_log.jsonl       every prompt, answer, model name and timing (LLM path only)
```

## Responsible AI safeguards built into the code

- The LLM sees legacy code, table structures and **made-up rows only**. `describe_failure` refuses to put
  values from non-synthetic data into a prompt (tested in `tests/test_tools.py`).
- AI-written code is checked before it runs: no file access, no extra imports, no JVM access (`engine/sandbox.py`).
  This is a guard rail, not a hardened sandbox.
- Every AI call is logged. AI-path lineage is labelled unverified. Every job starts with human review "pending".

## Known limits

- "Verified" means *matched on the data we tested*, not a mathematical proof.
- `CAST(float AS decimal)` can differ on exact ties (SQL Server uses the binary value, Spark the printed one).
- Text `<` / `>` and `MIN`/`MAX` on text follow Spark's ordering, not SQL Server's collation.
- `REPLACE` is case-sensitive here, case-insensitive under SQL Server's default collation.
- No SAS runtime is available, so SAS jobs have no legacy answers and stay *Needs review*.
- Generated code that divides decimals sets `spark.sql.decimalOperations.allowPrecisionLoss=false`.

## Data and tools declaration

- Data: freMTPL2 (public French motor insurance data, no personal identifiers); adversarial data is generated.
- Libraries: sqlglot (MIT), PySpark (Apache-2.0), pyodbc (MIT), PyYAML (MIT), pytest (MIT).
- AI tools: Claude Code was used to help write this codebase; `qwen2.5-coder:3b` via Ollama is the translation model.
