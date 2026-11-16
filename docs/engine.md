# Migration engine — legacy T-SQL / SAS to PySpark, with proof

This page documents the core engine (`engine/`). The repository overview is in the top-level README.

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
.venv\Scripts\python -m engine export
.venv\Scripts\python -m pytest
```

`--legacy golden --llm off` is the CI mode: no SQL Server, no model, only committed answer files.
`--record-golden` saves SQL Server's outputs as answer files.

Drop any `.sql` file into `jobs/` and it runs through the same pipeline; what cannot be handled
becomes *Needs review* with a reason. Nothing crashes.

`run` on one job also updates that job's entry in `out/status.json`, so a following `export` publishes it
without a full `run-all`. A folder under `jobs/` with no `legacy.sql` / `legacy.sas` (for example the CI fixture
`J00_mock`) is not a migration job and is skipped.

`jobs/J10` (`UNION ALL`) is a deliberately small job outside the rule engine: it shows the LLM path ending in
*Tested* when the local model's translation matches SQL Server on all three datasets.

## Job package (`jobs/<JOB_ID>/`)

`meta.yaml` follows the team's frozen interface contract; settings only the engine needs sit under `engine:`.

```yaml
job_id: J08
name: Policy dimension SCD Type 2 (MERGE)
source_dialect: tsql          # tsql | sas
expected_path: LLM            # IR | LLM (a declaration; the engine decides the real path itself)
trap_category: MERGE semantics; effective-date boundaries
sources:
  - table_name: dim_policy    # must exist in jobs/_schemas.yaml
    schema: dbo
primary_keys: []
engine:
  output_table: dbo.dim_policy   # legacy table holding the result; omit to take the last result set
  ci_columns: []                 # extra text columns compared ignoring case
columns: [...]                   # output columns; written by `export` for Verified / Tested jobs
```

## Publishing results in the team formats (`python -m engine export`)

`export` reads `out/` (it never re-runs a job) and writes the files below. All of them are run results and are
ignored by Git; CI regenerates them before it runs the tests.

```
ui/mock_data/status_store.json           status table for the UI and the Gold deployment gate
ui/mock_data/reconciliation/<job>.json   reconciliation report per job
jobs/<job>/oracle_output.parquet         legacy answer on the real-data sample   (Verified / Tested only)
jobs/<job>/solution.py                   generated code + run_job(spark)         (Verified / Tested only)
ui/mock_data/code/<job>.py               last PySpark attempt, whatever its level (for the dashboard)
ui/mock_data/lineage/<job>.json          lineage (for the dashboard)
ui/mock_data/ai_log/<job>.jsonl          every AI model call for the job (for the dashboard)
```

- `human_approval` is `approved: false` unless a valid sign-off is recorded (next section): approval is a
  human action, never a machine result.
- `category` in the status table comes from `category` in the job's `meta.yaml`.
- `rules_applied`, `out_of_scope_reason`, `model` and `seconds` are added for the dashboard; they are outside the team contract.
- A job that drops to *Needs review* has its exported `solution.py` / `oracle_output.parquet` removed,
  so the CI gate never checks stale code. Hand-written solutions are left alone.
- `solution.py` loads its inputs from `datasets/base/*.csv`, the same sample the answer file was made from.
- Deviation from the contract: `out/<job>/ir.json` keeps the engine's own typed node tree, not the v0
  `pipeline_ast` shape, because v0 stores expressions as text and cannot carry the type rules.

## Human sign-off (`python -m engine approve`)

The Gold deployment gate (`cloud/gold_deploy.py`) only promotes jobs that are Verified / Tested **and**
approved by a person. A reviewer reads the code and the reconciliation report in the dashboard, then runs:

```
.venv\Scripts\python -m engine approve J05 --reviewer "Your Name" --notes "read code and report"
.venv\Scripts\python -m engine revoke J05
```

- Refused for *Needs review* jobs, for a blank reviewer name, and for jobs with no published `solution.py`.
- Sign-offs are stored in `jobs/approvals.json` (not committed) together with the SHA-256 of the reviewed `solution.py`.
- A sign-off stops counting as soon as the published code changes or the job stops passing;
  the next `export` then writes `approved: false` with the reason in `notes`.

## Dashboard (`streamlit run ui/app.py`)

```
.venv\Scripts\python -m streamlit run ui\app.py
```

It shows what `export` published, so run `run-all` and `export` first. Navigation is a bar at the top.

- **Overview**: headline numbers, progress to the Gold layer, the job table (six columns by default; hover over a
  column name for an explanation), the Gold deployment gate, and a guide to levels and columns at the bottom.
- **Job detail**: legacy code next to PySpark with the legacy behaviours the rules reproduced; lineage; the
  reconciliation report opening with a plain verdict; the **AI audit** tab (every prompt and answer); and
  **Review & actions** (sign off, withdraw, re-run).
- **Analytics**: how many jobs were migrated by rules and by the AI model, measured machine time, an effort estimate
  based on an assumption the viewer sets, and blockers grouped by kind.
- **Try your own code**: paste or upload T-SQL / SAS to see whether the fixed rules can translate it, with the
  generated PySpark. Nothing is saved or run, and the AI model is not called.

Actions live in `ui/actions.py`; each long one (re-run, Gold deployment) starts a separate process.
There is no login: the reviewer name is recorded as typed. That suits a demo or a trusted team only.

## Settings (`.env`)

| Variable | Default | Used by |
|---|---|---|
| `MF_OLLAMA_HOST` | `http://localhost:11434` | engine, dashboard |
| `MF_OLLAMA_MODEL` | `qwen2.5-coder:3b` | engine, dashboard |
| `MF_SQLSERVER_CONN` | unset (answer files are used) | engine |
| `MF_LAKEHOUSE_ROOT` | `data` | `run_e2e_factory.py`, dashboard Gold deployment |

`MF_LAKEHOUSE_ROOT` is a folder path. For Azure storage, the `cloud/` scripts take `--cloud --cloud-prefix abfss://...`;
`infra/main.bicep` describes that storage. Neither has been run against Azure by us.

## Engine outputs (`out/`, not committed)

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

## Data and tools

- Data: freMTPL2 (public French motor insurance data, no personal identifiers); adversarial data is generated.
- Libraries: sqlglot (MIT), PySpark (Apache-2.0), pyodbc (MIT), PyYAML (MIT), pytest (MIT).
- Translation model: `qwen2.5-coder:3b`, run locally through Ollama.
