"""Run one legacy job through the whole factory and decide its verification level.
讓一個舊 job 走完整條工廠流程,並判定它的驗證等級。

Levels:
- Verified      whole job translated by fixed rules AND matches legacy on adversarial data
- Tested        needed the LLM, AND matches legacy on adversarial data
- Needs review  anything else (mismatch after 3 fixes, legacy cannot run, any error)
等級:
- Verified      整個 job 由固定規則翻譯,且在刁鑽資料上與舊系統一致
- Tested        用到 LLM,且在刁鑽資料上與舊系統一致
- Needs review  其餘情況(修 3 次仍不一致、舊系統跑不了、任何錯誤)

`run_job` never raises: every failure becomes a Needs review entry with a reason.
`run_job` 絕不丟出例外:任何失敗都會變成帶有原因的 Needs review。
"""
from __future__ import annotations

import datetime as dt
import json
import time
import traceback
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

from engine import ir
from engine.codegen import generate
from engine.job import Job, JobError, discover_jobs, load_job
from engine.legacy.runner import GoldenFileRunner, LegacyError, LegacyUnavailable
from engine.lineage import build_lineage, features, inferred_lineage
from engine.llm.client import LLMClient, LLMError, LoggedLLM
from engine.llm.translator import describe_failure, repair, translate
from engine.model import Column, Table
from engine.parser import parse_tsql
from engine.recon.advdata import empty_tables, generate_adversarial
from engine.recon.compare import CompareRules, compare
from engine.recon.shrink import shrink
from engine.sandbox import SandboxError, get_spark, run_code

VERIFIED, TESTED, NEEDS_REVIEW = "Verified", "Tested", "Needs review"
ADVERSARIAL = "adversarial"


@dataclass
class Context:
    """Everything a run needs that is not the job itself.
    一次執行所需、但不屬於 job 本身的所有東西。
    """

    schemas: dict[str, list[Column]]
    out_dir: Path
    golden: GoldenFileRunner = field(default_factory=GoldenFileRunner)
    live: object | None = None  # SqlServerRunner when SQL Server is reachable / 連得上 SQL Server 時為 SqlServerRunner
    llm: LLMClient | None = None
    record_golden: bool = False
    base: dict[str, Table] | None = None  # sample of the real public dataset / 真實公開資料的樣本
    max_repairs: int = 3
    shrink_trials: int = 60
    adversarial_rows: int = 40
    _spark: object | None = None

    @property
    def spark(self):
        if self._spark is None:
            self._spark = get_spark()
        return self._spark


def run_job(path: Path, ctx: Context) -> dict:
    """Process one job and return its status entry (also written to disk).
    處理一個 job 並回傳它的狀態紀錄(同時寫入磁碟)。
    """
    path = Path(path)
    started = time.time()
    entry = {
        "id": path.stem if path.is_file() else path.name,
        "title": "", "language": "", "trap": "",
        "path": None, "level": NEEDS_REVIEW, "reason": "", "out_of_scope_reason": None,
        "rules_applied": [], "features": [], "repairs": 0, "model": None, "datasets": [],
        "human_review": {"status": "pending", "reviewer": None, "comment": None},
        "stage": "load",
    }
    out = ctx.out_dir / entry["id"]
    try:
        job = load_job(path)
        entry.update(id=job.id, title=job.title, language=job.language, trap=job.trap)
        out = ctx.out_dir / job.id
        out.mkdir(parents=True, exist_ok=True)
        for stale in ("ir.json", "generated.py", "ai_log.jsonl", "recon_report.json", "lineage.json"):
            (out / stale).unlink(missing_ok=True)
        _process(job, ctx, entry, out)
    except JobError as exc:
        entry["reason"] = f"job could not be loaded: {exc}"
    except Exception as exc:  # last line of defence: an unknown job must never crash the factory
        entry["reason"] = f"internal error during '{entry['stage']}': {type(exc).__name__}: {exc}"
        entry["traceback"] = traceback.format_exc(limit=6)
    entry["seconds"] = round(time.time() - started, 1)
    entry.pop("stage", None)
    out.mkdir(parents=True, exist_ok=True)
    _write_json(out / "status.json", entry)
    return entry


def run_all(jobs_dir: Path, ctx: Context, only: list[str] | None = None) -> dict:
    """Process every job in a folder and write the combined status table.
    處理資料夾中的每個 job,並寫出彙總的狀態表。
    """
    entries = []
    for path in discover_jobs(jobs_dir):
        name = path.stem if path.is_file() else path.name
        if only and name not in only:
            continue
        entry = run_job(path, ctx)
        print(f"  {entry['id']:<10} {entry['level']:<13} {entry['path'] or '-':<4} {entry['seconds']:>6}s  {entry['reason']}", flush=True)
        entries.append(entry)
    return _write_status(ctx.out_dir, entries)


def merge_status(out_dir: Path, entry: dict) -> dict:
    """Put one job's fresh result into the combined status table, keeping every other job as it was.
    把單一 job 的新結果併入彙總狀態表,其他 job 維持原樣。

    Used after `run` on a single job, so that `export` publishes the re-run without a full `run-all`.
    A missing or unreadable table is rebuilt from this one entry.
    用於單一 job 的 `run` 之後,讓 `export` 不必重跑全部就能發布這次結果;狀態表不存在或無法讀取時,以這一筆重建。
    """
    path = Path(out_dir) / "status.json"
    try:
        entries = json.loads(path.read_text(encoding="utf-8"))["jobs"]
    except (FileNotFoundError, json.JSONDecodeError, KeyError, TypeError):
        entries = []
    entries = sorted([e for e in entries if e.get("id") != entry["id"]] + [entry], key=lambda e: e["id"])
    return _write_status(Path(out_dir), entries)


def _write_status(out_dir: Path, entries: list[dict]) -> dict:
    status = {
        "generated_at": dt.datetime.now(dt.timezone.utc).isoformat(timespec="seconds"),
        "summary": {level: sum(e["level"] == level for e in entries) for level in (VERIFIED, TESTED, NEEDS_REVIEW)},
        "jobs": entries,
    }
    _write_json(out_dir / "status.json", status)
    return status


# ---------- one job / 單一 job ----------

def _process(job: Job, ctx: Context, entry: dict, out: Path) -> None:
    unknown = [t for t in job.inputs if t not in ctx.schemas]
    if unknown:
        raise JobError(f"meta.yaml lists input tables that are not in the schema file: {unknown}")
    schemas = {t: ctx.schemas[t] for t in job.inputs} if job.inputs else dict(ctx.schemas)
    (out / ("legacy.sas" if job.language == "sas" else "legacy.sql")).write_text(job.legacy_code, encoding="utf-8")

    # 1. Rule path first; fall back to the LLM path with a stated reason.
    # 1. 先走規則路徑;走不通就帶著原因改走 LLM 路徑。
    entry["stage"] = "parse"
    query, code = None, None
    if job.language == "tsql":
        try:
            query = parse_tsql(job.legacy_code)
            generated = generate(query, schemas)
            code = generated.code
            entry.update(path="ir", rules_applied=generated.rules, features=features(query))
            _write_json(out / "ir.json", {"ir_version": ir.IR_VERSION, "query": ir.to_json(query)})
            _write_json(out / "lineage.json", {"job": job.id, **build_lineage(query, schemas)})
        except ir.OutOfScope as exc:
            query = None
            entry.update(path="llm", out_of_scope_reason=exc.reason)
    else:
        entry.update(path="llm", out_of_scope_reason="SAS program (IR v1 covers T-SQL only)")
    if entry["path"] == "llm":
        _write_json(out / "lineage.json", {"job": job.id, **inferred_lineage(job.legacy_code, schemas)})

    # 2. Test inputs and the legacy answers for them.
    # 2. 測試輸入,以及舊系統對這些輸入的答案。
    entry["stage"] = "legacy"
    datasets = _datasets(job, ctx, schemas, query)
    checked = [d for d in datasets if d["legacy"] is not None]
    rules = CompareRules(primary_key=job.primary_key, ci_columns=_ci_columns(job, query, schemas))
    report = {"job": job.id, "rules": {}, "attempts": [], "counterexample": None}

    # 3. Candidate code.
    # 3. 候選程式。
    entry["stage"] = "translate"
    llm = None
    if entry["path"] == "llm":
        if ctx.llm is None:
            entry["reason"] = f"outside IR scope ({entry['out_of_scope_reason']}) and the LLM path is switched off"
            _finish(entry, out, datasets, report, code)
            return
        llm = LoggedLLM(ctx.llm, out / "ai_log.jsonl", job.id)
        entry["model"] = llm.model
        expected_columns = checked[0]["legacy"].columns if checked else None
        try:
            llm.purpose = "translate"
            code = translate(job, schemas, llm, expected_columns)
        except LLMError as exc:
            entry["reason"] = f"the LLM could not produce a translation: {exc}"
            _finish(entry, out, datasets, report, code)
            return

    if not checked:
        why = "; ".join(sorted({d["legacy_status"] for d in datasets}))
        entry["reason"] = f"no legacy output to compare against ({why})"
        _finish(entry, out, datasets, report, code)
        return

    # 4. Reconcile; on the LLM path, repair up to max_repairs times.
    # 4. 對帳;LLM 路徑最多修正 max_repairs 次。
    entry["stage"] = "reconcile"
    failure = _evaluate(code, checked, rules, ctx, report, source="rules" if entry["path"] == "ir" else "translate")
    while failure is not None and entry["path"] == "llm" and entry["repairs"] < ctx.max_repairs:
        entry["stage"] = "repair"
        problem = _problem_text(job, code, failure, rules, ctx, report)
        entry["repairs"] += 1
        try:
            llm.purpose = f"repair {entry['repairs']}"
            code = repair(job, schemas, code, problem, llm, expected_columns)
        except LLMError as exc:
            report["attempts"].append({"source": f"repair {entry['repairs']}", "passed": False, "error": str(exc)})
            continue
        failure = _evaluate(code, checked, rules, ctx, report, source=f"repair {entry['repairs']}")

    if failure is None:
        if any(d["name"] == ADVERSARIAL for d in checked):
            entry["level"] = VERIFIED if entry["path"] == "ir" else TESTED
        else:
            entry["reason"] = "matches legacy on the available data, but adversarial data could not be checked against legacy"
    else:
        if entry["path"] == "ir":
            _problem_text(job, code, failure, rules, ctx, report)  # shrink for the human reviewer / 為審核者縮減反例
            entry["reason"] = f"rule-generated code differs from legacy on '{failure['dataset']}' data: {failure['summary']}"
        else:
            entry["reason"] = f"still differs from legacy after {entry['repairs']} repair(s): {failure['summary']}"
    report["rules"] = report["attempts"][-1].get("rules", {}) if report["attempts"] else {}
    _finish(entry, out, datasets, report, code)


def _finish(entry: dict, out: Path, datasets: list, report: dict, code: str | None) -> None:
    if code is not None:
        (out / "generated.py").write_text(code, encoding="utf-8")
    final = report["attempts"][-1]["datasets"] if report["attempts"] and "datasets" in report["attempts"][-1] else {}
    entry["datasets"] = [
        {
            "name": d["name"],
            "synthetic": d["synthetic"],
            "input_rows": {name: len(t.rows) for name, t in d["tables"].items()},
            "legacy_status": d["legacy_status"],
            "passed": final.get(d["name"], {}).get("passed"),
        }
        for d in datasets
    ]
    _write_json(out / "recon_report.json", report)


def _datasets(job: Job, ctx: Context, schemas: dict, query) -> list[dict]:
    """Build the test inputs and fetch the legacy output for each.
    建立測試輸入,並取得舊系統對每組輸入的輸出。
    """
    candidates = []
    if ctx.base is not None and all(t in ctx.base for t in schemas):
        candidates.append(("base", [{t: ctx.base[t] for t in schemas}]))
    # A legacy job may abort on some made-up rows (divide by zero); try a few seeds before giving up.
    # 舊 job 可能在某些虛構資料上中止(除以零);放棄前多試幾個亂數種子。
    candidates.append((ADVERSARIAL, [
        generate_adversarial(schemas, query, job.legacy_code, seed=seed, rows=ctx.adversarial_rows) for seed in range(4)
    ]))
    candidates.append(("empty", [empty_tables(schemas)]))

    datasets = []
    for name, options in candidates:
        chosen = {"name": name, "tables": options[0], "legacy": None, "legacy_status": ""}
        for tables in options:
            chosen["tables"] = tables
            try:
                chosen["legacy"] = _legacy(job, ctx, name, tables)
                chosen["legacy_status"] = "ok"
                break
            except LegacyError as exc:
                chosen["legacy_status"] = f"legacy error: {exc}"
            except LegacyUnavailable as exc:
                chosen["legacy_status"] = f"unavailable: {exc}"
                break
        chosen["synthetic"] = all(t.synthetic for t in chosen["tables"].values())
        datasets.append(chosen)
    return datasets


def _legacy(job: Job, ctx: Context, dataset: str, tables: dict[str, Table]) -> Table:
    if ctx.live is not None and job.language == "tsql":
        output = ctx.live.run(job, tables, dataset)
        if ctx.record_golden:
            ctx.golden.save(job, tables, dataset, output)
        return output
    return ctx.golden.run(job, tables, dataset)


def _evaluate(code: str, checked: list, rules: CompareRules, ctx: Context, report: dict, source: str):
    """Run the code on every checked dataset. Returns None when all match, else the first failure.
    在每組有答案的資料上執行程式;全部一致回傳 None,否則回傳第一個失敗。
    """
    attempt = {"source": source, "passed": True, "datasets": {}}
    failure = None
    for d in checked:
        try:
            new = run_code(code, ctx.spark, d["tables"])
            result = compare(d["legacy"], new, rules)
            attempt["rules"] = result["rules"]
        except SandboxError as exc:
            new, result = None, {"passed": False, "error": str(exc)}
        attempt["datasets"][d["name"]] = result
        if not result["passed"] and failure is None:
            attempt["passed"] = False
            failure = {"dataset": d["name"], "data": d, "new": new, "result": result, "summary": _summary(result)}
    report["attempts"].append(attempt)
    return failure


def _problem_text(job: Job, code: str, failure: dict, rules: CompareRules, ctx: Context, report: dict) -> str:
    """Describe a failure for the repair prompt, shrinking the inputs first when possible.
    為修正用的 prompt 描述失敗情況;可行時先縮減輸入。
    """
    if failure["new"] is None:
        return "The code failed to run: " + failure["result"]["error"]
    data = failure["data"]
    tables, legacy, new, result = data["tables"], data["legacy"], failure["new"], failure["result"]
    can_shrink = data["synthetic"] and ctx.live is not None and job.language == "tsql" and ctx.shrink_trials > 0
    if can_shrink:
        def still_fails(candidate: dict[str, Table]) -> bool:
            try:
                expected = ctx.live.run(job, candidate, "shrink")
                return not compare(expected, run_code(code, ctx.spark, candidate), rules)["passed"]
            except (LegacyError, LegacyUnavailable, SandboxError):
                return False

        small, trials = shrink(tables, still_fails, ctx.shrink_trials)
        try:
            small_legacy = ctx.live.run(job, small, "shrink")
            small_new = run_code(code, ctx.spark, small)
            small_result = compare(small_legacy, small_new, rules)
            if not small_result["passed"]:
                tables, legacy, new, result = small, small_legacy, small_new, small_result
                report["counterexample"] = {
                    "dataset": failure["dataset"],
                    "shrunk_from_rows": sum(len(t.rows) for t in data["tables"].values()),
                    "shrunk_to_rows": sum(len(t.rows) for t in small.values()),
                    "reruns": trials,
                    "inputs": {name: _table_json(t) for name, t in small.items()},
                    "legacy_output": _table_json(small_legacy),
                    "new_output": _table_json(small_new),
                }
        except (LegacyError, LegacyUnavailable, SandboxError):
            pass  # keep the unshrunk failure; shrinking is an aid, not a requirement / 保留未縮減的失敗;縮減只是輔助
    return describe_failure(tables, legacy, new, result)


def _ci_columns(job: Job, query, schemas) -> list[str]:
    """Text outputs compared ignoring case: those declared in meta.yaml plus text GROUP BY outputs.
    比對時不分大小寫的文字輸出:meta.yaml 宣告的,加上以文字做 GROUP BY 的輸出欄位。
    """
    columns = list(job.ci_columns)
    if query is not None and query.group_by:
        from engine.ir_types import infer, query_scope, resolve_query

        resolved = resolve_query(query, schemas)
        scope = query_scope(resolved, schemas)
        keys = {k for k in resolved.group_by if infer(k, scope) == "string"}
        columns += [o.name for o in resolved.select if o.expr in keys and o.name not in columns]
    return columns


def _summary(result: dict) -> str:
    if "error" in result:
        return "code failed to run: " + result["error"][:160]
    checks = result["checks"]
    parts = []
    if not checks["schema"]["ok"]:
        parts.append(f"columns differ (missing {checks['schema']['missing_in_new']}, extra {checks['schema']['extra_in_new']})")
    if not checks["row_count"]["ok"]:
        parts.append(f"row count {checks['row_count']['legacy']} vs {checks['row_count']['new']}")
    rows = checks["rows"]
    if not rows["ok"]:
        parts.append(f"{rows['missing']} missing, {rows['extra']} extra, {rows['different']} different rows")
    return "; ".join(parts) or "totals differ"


def _table_json(table: Table) -> dict:
    return {"columns": [f"{c.name}:{c.type}" for c in table.columns], "rows": [list(row) for row in table.rows]}


def _write_json(path: Path, data) -> None:
    def default(value):
        if isinstance(value, Decimal):
            return str(value)
        if isinstance(value, (dt.date, dt.datetime)):
            return value.isoformat()
        raise TypeError(f"cannot write {type(value).__name__} to JSON")

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False, default=default), encoding="utf-8")
