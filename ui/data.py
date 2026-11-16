"""Read-only data access for the dashboard: status table, reconciliation reports, code and lineage.
儀表板的唯讀資料存取:狀態表、對帳報告、程式碼與 lineage。

Nothing here imports Streamlit or the engine, so it can be tested on its own.
這裡不 import Streamlit 也不 import 引擎,因此可以單獨測試。
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]
LEVELS = ("Verified", "Tested", "Needs review")
_JOB_ID = re.compile(r"^[A-Za-z0-9_]+$")


class UiDataError(Exception):
    """A file the dashboard needs is missing or unreadable.
    儀表板需要的檔案不存在或無法讀取。
    """


def _safe_id(job_id: str) -> str:
    # Job ids become folder names, so anything that could leave the folder is refused.
    # job id 會被當成資料夾名稱,因此拒絕任何可能跳出資料夾的字元。
    if not _JOB_ID.match(job_id or ""):
        raise UiDataError(f"invalid job id {job_id!r}")
    return job_id


def _read_json(path: Path, what: str) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError as error:
        raise UiDataError(f"{what} not found at {path}; run `python -m engine export` first") from error
    except json.JSONDecodeError as error:
        raise UiDataError(f"{what} at {path} is not valid JSON: {error}") from error


def load_store(ui_dir: Path) -> dict:
    """Load the status table written by `engine export`.
    載入 `engine export` 寫出的狀態表。
    """
    store = _read_json(Path(ui_dir) / "status_store.json", "status table")
    if not isinstance(store.get("jobs"), list) or "metrics" not in store:
        raise UiDataError("status table has no `jobs` list or no `metrics`")
    return store


def job_rows(store: dict) -> list[dict]:
    """Flatten the job list into one row per job for the overview table.
    將 job 清單攤平成總覽表的一列一個 job。
    """
    rows = []
    for job in store["jobs"]:
        approval = job.get("human_approval") or {}
        rows.append({
            "Job": job["job_id"],
            "Name": job.get("name", ""),
            "Category": job.get("category", ""),
            "Dialect": job.get("dialect", ""),
            "Path": job.get("path", ""),
            "Status": job.get("status", ""),
            "Reconciled": bool(job.get("reconciliation_passed")),
            "Discrepancies": job.get("discrepancies"),
            "LLM iterations": job.get("llm_iterations"),
            "Human approved": bool(approval.get("approved")),
            "Reviewer": approval.get("reviewer") or "",
            "Reason": job.get("reason", ""),
        })
    return rows


def load_report(ui_dir: Path, job_id: str) -> dict | None:
    """Reconciliation report of one job, or None when the job has none.
    單一 job 的對帳報告;沒有則回傳 None。
    """
    path = Path(ui_dir) / "reconciliation" / f"{_safe_id(job_id)}.json"
    return _read_json(path, "reconciliation report") if path.exists() else None


def load_job_files(root: Path, job_id: str) -> dict:
    """Legacy source, migrated PySpark (if any) and meta.yaml of one job.
    單一 job 的舊程式、遷移後的 PySpark(若有)與 meta.yaml。

    The migrated code comes from the published `solution.py` first, then from the last attempt that
    `engine export` copied next to the status table.
    遷移後的程式優先取已發布的 `solution.py`,其次取 `engine export` 複製到狀態表旁的最後一次嘗試。
    """
    folder = Path(root) / "jobs" / _safe_id(job_id)
    legacy = next((p for p in (folder / "legacy.sql", folder / "legacy.sas") if p.exists()), None)
    if legacy is None:
        raise UiDataError(f"job {job_id} has no legacy.sql or legacy.sas in {folder}")
    meta_path = folder / "meta.yaml"
    meta = yaml.safe_load(meta_path.read_text(encoding="utf-8")) if meta_path.exists() else {}

    generated, origin = None, None
    candidates = (
        (folder / "solution.py", "published solution.py (passed reconciliation)"),
        (Path(root) / "ui" / "mock_data" / "code" / f"{job_id}.py", "last attempt (did not pass reconciliation)"),
    )
    for path, label in candidates:
        if path.exists():
            generated, origin = path.read_text(encoding="utf-8"), label
            break
    return {
        "legacy": legacy.read_text(encoding="utf-8"),
        "legacy_language": "sql" if legacy.suffix == ".sql" else "sas",
        "generated": generated,
        "generated_origin": origin,
        "meta": meta or {},
    }


def load_lineage(root: Path, job_id: str, meta: dict) -> dict:
    """Lineage published by `engine export`, else table-level lineage from meta.yaml.
    優先取 `engine export` 發布的 lineage,否則用 meta.yaml 的表級 lineage。
    """
    path = Path(root) / "ui" / "mock_data" / "lineage" / f"{_safe_id(job_id)}.json"
    if path.exists():
        lineage = _read_json(path, "lineage")
        lineage.setdefault("outputs", {})
        lineage.setdefault("tables", [])
        return lineage
    return {
        "job": job_id,
        "method": "meta.yaml",
        "confidence": "table level only (declared, not derived)",
        "tables": [s["table_name"] for s in meta.get("sources", []) if "table_name" in s],
        "outputs": {c["name"]: {"sources": [], "expression": "", "operations": []} for c in meta.get("columns", [])},
    }


def _quote(text: str) -> str:
    return '"' + str(text).replace("\\", "\\\\").replace('"', '\\"') + '"'


def lineage_dot(lineage: dict, job_id: str) -> str:
    """Graphviz DOT for the lineage: source column -> output column, or table -> job when columns are unknown.
    lineage 的 Graphviz DOT:來源欄位 -> 輸出欄位;欄位未知時為 表 -> job。
    """
    lines = ["digraph lineage {", "  rankdir=LR;", '  node [shape=box, fontsize=10, style="rounded"];']
    edges = [(src, out) for out, info in lineage.get("outputs", {}).items() for src in info.get("sources", [])]
    if edges:
        for source, output in edges:
            lines.append(f"  {_quote(source)} -> {_quote(job_id + '.' + output)};")
    else:
        for table in lineage.get("tables", []):
            lines.append(f"  {_quote(table)} -> {_quote(job_id)};")
    lines.append("}")
    return "\n".join(lines)


def breakdown(store: dict, field: str) -> dict[str, dict[str, int]]:
    """Count jobs per value of `field` (e.g. dialect, path), split by status level.
    依 `field`(例如 dialect、path)統計 job 數,並按狀態等級拆分。
    """
    counts: dict[str, dict[str, int]] = {}
    for job in store["jobs"]:
        group = counts.setdefault(str(job.get(field) or "Unknown"), {level: 0 for level in LEVELS})
        status = job.get("status")
        group[status if status in LEVELS else "Needs review"] += 1
    return counts


def gold_ready(store: dict) -> list[str]:
    """Jobs the Gold deploy would accept: reconciled level AND approved by a human.
    Gold 部署會接受的 job:對帳等級通過「且」已人工核可。
    """
    return [
        job["job_id"] for job in store["jobs"]
        if job.get("status") in ("Verified", "Tested") and (job.get("human_approval") or {}).get("approved") is True
    ]


def load_ai_log(ui_dir: Path, job_id: str) -> list[dict]:
    """Every call made to the AI model for one job, oldest first. Empty when the model was never asked.
    單一 job 對 AI 模型的每一次呼叫,由舊到新;從未呼叫模型時為空。
    """
    path = Path(ui_dir) / "ai_log" / f"{_safe_id(job_id)}.jsonl"
    if not path.exists():
        return []
    calls = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        if not line.strip():
            continue
        try:
            calls.append(json.loads(line))
        except json.JSONDecodeError as error:
            raise UiDataError(f"AI log at {path} line {number} is not valid JSON: {error}") from error
    return calls


def funnel(store: dict) -> dict[str, int]:
    """How far the jobs have come: all -> reconciled -> signed off by a person.
    job 走到哪一步:全部 -> 對帳通過 -> 已有人簽核。
    """
    jobs = store["jobs"]
    return {
        "jobs": len(jobs),
        "reconciled": sum(job.get("status") in ("Verified", "Tested") for job in jobs),
        "signed_off": len(gold_ready(store)),
    }


def next_step(job: dict) -> str:
    """One sentence telling a reviewer what this job needs next.
    用一句話告訴審閱者這個 job 接下來需要什麼。
    """
    if job.get("status") in ("Verified", "Tested"):
        if (job.get("human_approval") or {}).get("approved"):
            return "Approved. The next release will promote this job to the Gold layer."
        if job.get("status") == "Tested":
            return "AI-translated and reconciled. Read the code and the AI audit trail, then sign it off."
        return "Reconciled by fixed rules. Read the code and the report, then sign it off."
    return "Not ready: " + blocker_type(job.get("reason", "")) + ". See the Reconciliation and AI audit tabs."


def blocker_type(reason: str) -> str:
    """Group a free-text reason into a short kind of blocker, for counting.
    把自由文字的原因歸成簡短的阻擋類型,以便統計。
    """
    text = (reason or "").lower()
    if "no legacy output" in text:
        return "No legacy answer to compare against"
    if "llm path is switched off" in text:
        return "Needs the AI model, which was switched off"
    if "still differs" in text or "could not produce" in text:
        return "AI translation did not match legacy"
    if "rule-generated code differs" in text:
        return "Rule translation did not match legacy"
    if "could not be loaded" in text or "internal error" in text:
        return "Job could not be processed"
    return "Other"


def report_summary(report: dict | None) -> str:
    """Plain-English verdict of a reconciliation report.
    對帳報告的白話結論。
    """
    if not report:
        return "No reconciliation report exists for this job."
    datasets = report.get("datasets") or []
    checked = [d for d in datasets if d.get("passed") is not None]
    summary = report.get("summary") or {}
    if not checked or summary.get("legacy_rows") is None:
        return "Nothing was compared: there is no legacy output for this job."
    passed = sum(bool(d.get("passed")) for d in checked)
    if report.get("overall_passed"):
        return (f"All {summary['legacy_rows']} legacy rows match on the sample data, "
                f"and {passed} of {len(checked)} test datasets pass.")
    return (f"The outputs differ: {summary.get('mismatched_rows')} mismatched rows, row counts "
            f"{summary['legacy_rows']} (legacy) vs {summary.get('spark_rows')} (Spark); "
            f"{passed} of {len(checked)} test datasets pass.")
