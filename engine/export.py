"""Publish the engine's results in the team's frozen interface formats.
把引擎的結果轉寫成團隊凍結的介面格式。

Reads what `run-all` left in the output folder and writes, without re-running any job:
- ui/mock_data/status_store.json            the status table read by the UI and the Gold deployment gate
- ui/mock_data/reconciliation/<job>.json    one reconciliation report per job
- jobs/<job>/oracle_output.parquet          the legacy answer on the real-data sample
- jobs/<job>/solution.py                    the PySpark code with a `run_job(spark)` entry point for the CI gate
- ui/mock_data/code/<job>.py                the last PySpark attempt, whatever its level (for the dashboard)
- ui/mock_data/lineage/<job>.json           the job's lineage (for the dashboard)
The Parquet answer and solution.py are written only for jobs whose level is Verified or Tested.
讀取 `run-all` 留在輸出資料夾的結果並寫出下列檔案,不會重跑任何 job:
- ui/mock_data/status_store.json            UI 與 Gold 部署門禁讀取的狀態表
- ui/mock_data/reconciliation/<job>.json    每個 job 一份對帳報告
- jobs/<job>/oracle_output.parquet          舊系統在真實資料樣本上的答案
- jobs/<job>/solution.py                    帶有 `run_job(spark)` 進入點的 PySpark 程式,供 CI 門禁使用
- ui/mock_data/code/<job>.py                最後一次產生的 PySpark,不論等級(給儀表板看)
- ui/mock_data/lineage/<job>.json           job 的 lineage(給儀表板看)
Parquet 答案與 solution.py 只為等級是 Verified 或 Tested 的 job 產生。
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import yaml

from engine.approval import PASSING_LEVELS, ApprovalError, approval_for
from engine.job import Job, JobError, load_job
from engine.legacy.runner import golden_dir
from engine.model import Column, Table, decimal_parts, is_decimal, read_table

SOLUTION_MARKER = "# AUTO-EXPORTED by engine/export.py"
BASE_DATASET = "base"
SAMPLE_LIMIT = 3
_DIALECT_LABELS = {"tsql": "T-SQL", "sas": "SAS"}
_CONTRACT_TYPES = {"int": "INT", "bigint": "BIGINT", "float": "FLOAT", "string": "VARCHAR", "bool": "BIT", "date": "DATE"}
_SPARK_TYPES = {"int": "int", "bigint": "bigint", "float": "double", "string": "string", "date": "date"}


class ExportError(Exception):
    """One job (or the whole run) could not be exported; the message says which and why.
    某個 job(或整次執行)無法匯出;訊息會說明是哪一個以及原因。
    """


def export_all(out_dir: Path, jobs_dir: Path, base_dir: Path, ui_dir: Path, schemas: dict[str, list[Column]]) -> dict:
    """Export every job listed in `<out_dir>/status.json`.
    匯出 `<out_dir>/status.json` 列出的每個 job。

    Returns {"store": <status table>, "gate_jobs": [...], "problems": {job: reason}}.
    A problem with one job never stops the others; callers must treat a non-empty `problems` as failure.
    回傳 {"store": 狀態表, "gate_jobs": [...], "problems": {job: 原因}}。
    單一 job 出問題不會中斷其他 job;`problems` 非空時呼叫端必須視為失敗。
    """
    status_path = out_dir / "status.json"
    if not status_path.exists():
        raise ExportError(f"{status_path} not found; run `python -m engine run-all` first")
    status = json.loads(status_path.read_text(encoding="utf-8"))
    now = dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds")

    records, gate_jobs, problems = [], [], {}
    for entry in status.get("jobs", []):
        job_id = entry["id"]
        # Reasons can quote absolute paths of the machine that ran the jobs; publish them relative to the project.
        # 原因文字可能帶有執行機器上的絕對路徑;對外發布時改成相對於專案的路徑。
        entry = {**entry, "reason": _relative_paths(entry.get("reason") or "", jobs_dir.resolve().parent)}
        report = _read_json(out_dir / job_id / "recon_report.json") or {}
        _write_json(ui_dir / "reconciliation" / f"{job_id}.json", reconciliation_report(entry, report, now))
        _publish_for_dashboard(out_dir / job_id, ui_dir, job_id)
        job_folder = jobs_dir / job_id
        if not job_folder.is_dir():
            records.append(status_record(entry, report))
            continue  # a bare .sql/.sas file has no package folder to publish into
        try:
            if entry.get("level") in PASSING_LEVELS:
                _publish_gate_package(load_job(job_folder), out_dir / job_id, base_dir, schemas)
                gate_jobs.append(job_id)
            else:
                _remove_gate_package(job_folder)
        except (ExportError, JobError, OSError, ValueError) as exc:
            # Leave no half-written package behind for the CI gate to pick up.
            # 不留下寫到一半的套件讓 CI 門禁撿到。
            _remove_gate_package(job_folder)
            problems[job_id] = str(exc)
        # Built after the package is settled: an approval only holds for the code that is published right now.
        # 等套件定案後才組這一列:核可只對「此刻已發布」的那份程式有效。
        try:
            approval = approval_for(jobs_dir, job_id, entry.get("level", "Needs review"))
        except ApprovalError as exc:
            approval = None
            problems[job_id] = str(exc)
        records.append(status_record(entry, report, approval, _category(job_folder)))

    store = {"updated_at": now, "metrics": _metrics(records), "jobs": records}
    _write_json(ui_dir / "status_store.json", store)
    return {"store": store, "gate_jobs": gate_jobs, "problems": problems}


def status_record(entry: dict, report: dict, approval: dict | None = None, category: str | None = None) -> dict:
    """One row of the status table. Without a recorded sign-off the job is not approved.
    狀態表的一列。沒有簽核記錄時,job 一律是「未核可」。
    """
    level = entry.get("level", "Needs review")
    path = entry.get("path")
    record = {
        "job_id": entry["id"],
        "name": entry.get("title") or entry["id"],
        "category": category or "Unclassified",
        "dialect": _DIALECT_LABELS.get(entry.get("language"), str(entry.get("language") or "unknown")),
        "path": path.upper() if path else None,
        "status": level,
        "reconciliation_passed": level in PASSING_LEVELS,
        "discrepancies": _discrepancies(report),
        "reason": entry.get("reason") or "",
        "human_approval": approval or {"approved": False, "reviewer": None, "timestamp": None, "notes": ""},
    }
    if path == "llm":
        record["llm_iterations"] = len(report.get("attempts", []))
    return record


def reconciliation_report(entry: dict, report: dict, timestamp: str) -> dict:
    """The per-job report in the contract's shape, built from the engine's own report.
    依契約格式組出單一 job 的報告,資料來自引擎自己的報告。
    """
    attempts = report.get("attempts", [])
    datasets = attempts[-1].get("datasets", {}) if attempts else {}
    name, focus = _focus_dataset(datasets)
    checks = (focus or {}).get("checks", {})
    rows, count = checks.get("rows", {}), checks.get("row_count", {})
    path = entry.get("path")
    result = {
        "job_id": entry["id"],
        "timestamp": timestamp,
        "execution_path": path.upper() if path else None,
        "overall_passed": entry.get("level") in PASSING_LEVELS,
        "iteration_count": len(attempts),
        "reason": entry.get("reason") or "",
        "dataset": name,
        "summary": {
            "legacy_rows": count.get("legacy"),
            "spark_rows": count.get("new"),
            "count_diff": None if not count else count["new"] - count["legacy"],
            "mismatched_rows": None if not rows else rows["missing"] + rows["extra"] + rows["different"],
        },
        "field_checks": [
            {"column": column, "status": "PASSED" if total["ok"] else "FAILED",
             "legacy_total": total["legacy"], "spark_total": total["new"]}
            for column, total in checks.get("totals", {}).items()
        ],
        "counterexamples": _counterexamples((focus or {}).get("samples", {})),
        "datasets": [
            {"name": key, "passed": value.get("passed"), "legacy_status": value.get("legacy_status")}
            for key, value in datasets.items()
        ],
        "shrinking_metadata": None,
    }
    shrunk = report.get("counterexample")
    if shrunk:
        result["shrinking_metadata"] = {
            "dataset": shrunk.get("dataset"),
            "original_input_rows": shrunk.get("shrunk_from_rows"),
            "shrunk_input_rows": shrunk.get("shrunk_to_rows"),
            "reruns": shrunk.get("reruns"),
            "inputs": shrunk.get("inputs"),
            "legacy_output": shrunk.get("legacy_output"),
            "spark_output": shrunk.get("new_output"),
        }
    return result


def build_solution(generated_code: str, job: Job, schemas: dict[str, list[Column]]) -> str:
    """Append a `run_job(spark)` entry point that loads the committed sample itself.
    附加一個會自行載入已提交樣本的 `run_job(spark)` 進入點。
    """
    if not job.inputs:
        raise ExportError(f"{job.id}: meta.yaml declares no `sources`, so the inputs to load are unknown")
    ddl = {}
    for table in job.inputs:
        if table not in schemas:
            raise ExportError(f"{job.id}: input table {table!r} is not in the schema file")
        ddl[table] = ", ".join(f"`{c.name}` {_spark_type(job.id, c)}" for c in schemas[table])
    loader = f'''

{SOLUTION_MARKER} from the engine's output for job {job.id}. Do not edit by hand.
# Everything above is the migrated job; the code below only loads its inputs for the CI gate.
# 以上是轉換後的 job;以下只負責為 CI 門禁載入輸入資料。
from pathlib import Path as _Path

_BASE_DIR = _Path(__file__).resolve().parents[2] / "datasets" / "base"
_INPUTS = {json.dumps(ddl, indent=4)}


def run_job(spark):
    """Run the migrated job on the committed real-data sample (datasets/base).
    在已提交的真實資料樣本(datasets/base)上執行轉換後的 job。
    """
    if spark is None:
        raise RuntimeError("job {job.id} needs a SparkSession; there is no non-Spark fallback")
    tables = {{}}
    for name, schema in _INPUTS.items():
        path = _BASE_DIR / f"{{name}}.csv"
        if not path.exists():
            raise FileNotFoundError(f"job {job.id}: input sample {{path}} is missing")
        tables[name] = spark.read.csv(str(path), schema=schema, header=True, nullValue="\\\\N", quote='"', escape='"')
    return run(spark, tables)
'''
    return generated_code.rstrip("\n") + "\n" + loader


def contract_columns(table: Table) -> list[dict]:
    """Describe output columns the way the contract's meta.yaml does.
    依契約 meta.yaml 的寫法描述輸出欄位。
    """
    columns = []
    for column in table.columns:
        if is_decimal(column.type):
            columns.append({"name": column.name, "type": column.type.upper(), "compare_mode": "exact"})
        elif column.type == "float":
            columns.append({"name": column.name, "type": "FLOAT", "compare_mode": "tolerance", "abs_tolerance": 1.0e-9})
        elif column.type in _CONTRACT_TYPES:
            columns.append({"name": column.name, "type": _CONTRACT_TYPES[column.type], "compare_mode": "exact"})
        else:
            raise ExportError(f"output column {column.name!r} has a type the contract cannot express: {column.type!r}")
    return columns


def write_parquet(table: Table, path: Path) -> None:
    """Write a table as Parquet, keeping exact decimal types.
    把表寫成 Parquet,保留精確的小數型別。
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    def arrow_type(type_name: str):
        if is_decimal(type_name):
            return pa.decimal128(*decimal_parts(type_name))
        simple = {"int": pa.int32(), "bigint": pa.int64(), "float": pa.float64(),
                  "string": pa.string(), "bool": pa.bool_(), "date": pa.date32()}
        if type_name not in simple:
            raise ExportError(f"no Parquet type for {type_name!r}")
        return simple[type_name]

    arrays = [
        pa.array([row[i] for row in table.rows], type=arrow_type(column.type))
        for i, column in enumerate(table.columns)
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(pa.Table.from_arrays(arrays, names=table.names), path)


def _publish_gate_package(job: Job, job_out: Path, base_dir: Path, schemas: dict[str, list[Column]]) -> None:
    folder = job.source_path.parent
    if not job.primary_key:
        raise ExportError(f"{job.id}: no primary key, which the CI gate requires")
    generated = job_out / "generated.py"
    answer = golden_dir(job) / f"{BASE_DATASET}.csv"
    for needed in (generated, answer, *(base_dir / f"{t}.csv" for t in job.inputs)):
        if not needed.exists():
            raise ExportError(f"{job.id}: {needed} is missing")
    oracle = read_table(answer)
    solution = build_solution(generated.read_text(encoding="utf-8"), job, schemas)

    meta_path = folder / "meta.yaml"
    meta = yaml.safe_load(meta_path.read_text(encoding="utf-8")) or {}
    meta["columns"] = contract_columns(oracle)
    meta_path.write_text(yaml.safe_dump(meta, sort_keys=False, allow_unicode=True), encoding="utf-8")
    write_parquet(oracle, folder / "oracle_output.parquet")
    (folder / "solution.py").write_text(solution, encoding="utf-8", newline="\n")


def _publish_for_dashboard(job_out: Path, ui_dir: Path, job_id: str) -> None:
    """Copy the last PySpark attempt and the lineage next to the status table, or remove stale copies.
    把最後一次的 PySpark 與 lineage 複製到狀態表旁;來源不存在時移除過期的副本。
    """
    for source, target in ((job_out / "generated.py", ui_dir / "code" / f"{job_id}.py"),
                           (job_out / "lineage.json", ui_dir / "lineage" / f"{job_id}.json")):
        if source.exists():
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(source.read_text(encoding="utf-8"), encoding="utf-8", newline="\n")
        else:
            target.unlink(missing_ok=True)


def _category(job_folder: Path) -> str | None:
    """The job's `category` from meta.yaml, if it has one.
    meta.yaml 裡的 `category`(若有)。
    """
    meta_path = job_folder / "meta.yaml"
    if not meta_path.exists():
        return None
    meta = yaml.safe_load(meta_path.read_text(encoding="utf-8")) or {}
    return str(meta["category"]) if meta.get("category") else None


def _remove_gate_package(folder: Path) -> None:
    """Remove a previously exported package so the CI gate never checks stale code.
    移除先前匯出的套件,確保 CI 門禁不會驗到過期的程式。

    Only files this module wrote are removed: a hand-written solution.py is left alone.
    只移除本模組寫出的檔案:人工撰寫的 solution.py 不會被動到。
    """
    solution = folder / "solution.py"
    if solution.exists() and SOLUTION_MARKER not in solution.read_text(encoding="utf-8"):
        return
    solution.unlink(missing_ok=True)
    (folder / "oracle_output.parquet").unlink(missing_ok=True)


def _focus_dataset(datasets: dict) -> tuple[str | None, dict | None]:
    """The dataset a reader should look at first: the first failing one, else the real-data sample.
    讀者最該先看的資料集:第一個沒過的,否則是真實資料樣本。
    """
    compared = {name: d for name, d in datasets.items() if "checks" in d}
    for name, data in compared.items():
        if not data.get("passed"):
            return name, data
    if BASE_DATASET in compared:
        return BASE_DATASET, compared[BASE_DATASET]
    return next(iter(compared.items()), (None, None))


def _discrepancies(report: dict) -> int | None:
    """Rows that differ in the focus dataset; None when nothing could be compared.
    重點資料集中不一致的列數;完全無法比對時為 None。
    """
    attempts = report.get("attempts", [])
    _, focus = _focus_dataset(attempts[-1].get("datasets", {}) if attempts else {})
    if focus is None:
        return None
    rows = focus["checks"].get("rows", {})
    return rows.get("missing", 0) + rows.get("extra", 0) + rows.get("different", 0)


def _counterexamples(samples: dict) -> list[dict]:
    found = []
    for item in samples.get("different", []):
        found.append({"kind": "value_mismatch", "primary_key": item.get("key"), "cells": item.get("cells")})
    for item in samples.get("missing", []):
        found.append({"kind": "missing_in_spark", "legacy_value": item.get("row", item), "spark_value": None})
    for item in samples.get("extra", []):
        found.append({"kind": "extra_in_spark", "legacy_value": None, "spark_value": item.get("row", item)})
    return found[:SAMPLE_LIMIT]


def _metrics(records: list[dict]) -> dict:
    counts = {level: sum(1 for r in records if r["status"] == level) for level in (*PASSING_LEVELS, "Needs review")}
    total = len(records)
    passing = counts["Verified"] + counts["Tested"]
    return {
        "total_jobs": total,
        "verified": counts["Verified"],
        "tested": counts["Tested"],
        "needs_review": total - passing,
        "pass_rate_pct": round(100 * passing / total, 2) if total else 0.0,
    }


def _spark_type(job_id: str, column: Column) -> str:
    if is_decimal(column.type):
        return column.type
    if column.type not in _SPARK_TYPES:
        raise ExportError(f"{job_id}: input column {column.name!r} of type {column.type!r} cannot be loaded from the CSV sample")
    return _SPARK_TYPES[column.type]


def _relative_paths(text: str, root: Path) -> str:
    for prefix in (str(root) + "\\", str(root).replace("\\", "\\\\") + "\\\\", root.as_posix() + "/"):
        text = text.replace(prefix, "")
    return text


def _read_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def _write_json(path: Path, data) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=str) + "\n", encoding="utf-8")
