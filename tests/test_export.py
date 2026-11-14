"""Tests for engine/export.py (publishing results in the team's interface formats).
engine/export.py 的測試(依團隊介面格式發布結果)。

Test list / 測試清單
- test_approval_always_starts_unapproved
    Purpose: a machine result must never arrive already "approved by a human".
    Method: build status records for Verified and Needs-review jobs; expect approved == False, no reviewer.
- test_only_matching_levels_count_as_reconciled
    Purpose: Needs review may not be reported as reconciled.
    Method: records for each level; expect reconciliation_passed only for Verified / Tested.
- test_metrics_add_up
    Purpose: the headline numbers must equal what the job list says.
    Method: export 1 Verified + 1 Needs review; expect verified=1, needs_review=1, pass rate 50.0.
- test_verified_job_gets_a_gate_package
    Purpose: a Verified job is published for the CI gate with an exact answer file and a runnable entry point.
    Method: export; expect Parquet keeps Decimal('10.50') and NULL, solution.py compiles and defines run_job,
            meta.yaml gains the output columns (decimal exact, float with 1e-9 tolerance).
- test_needs_review_job_is_withdrawn_from_the_gate
    Purpose: code that no longer matches legacy must not stay in front of the CI gate.
    Method: pre-place an auto-exported solution.py + parquet in a Needs-review job; export; expect both gone.
- test_hand_written_solution_is_never_deleted
    Purpose: export must not destroy a teammate's hand-written solution.
    Method: pre-place a solution.py without the export marker in a Needs-review job; export; expect it intact.
- test_job_without_primary_key_is_reported_not_published
    Purpose: the CI gate fails packages without a key, so such a job is reported instead of published.
    Method: Verified job with no primary_keys; expect it in `problems`, no solution.py, still in the status table.
- test_missing_run_results_is_an_error
    Purpose: exporting before running must fail loudly instead of writing an empty status table.
    Method: empty out folder; expect ExportError mentioning run-all.
- test_report_focuses_on_the_failing_dataset
    Purpose: the report's summary must show the failure, not a dataset that happened to pass.
    Method: last attempt has base passing and adversarial failing; expect dataset == adversarial,
            mismatched_rows == 5, FAILED field check, at most 3 counterexamples, shrinking info carried over.
- test_machine_paths_are_not_published
    Purpose: published reasons must not carry absolute paths of the machine that ran the jobs.
    Method: reason containing the project's absolute path; expect a project-relative path in the status table.
- test_category_comes_from_meta
    Purpose: the status table shows the job's declared category, and says so when none is declared.
    Method: one job with `category: Cleansing` in meta.yaml, one without; expect "Cleansing" and "Unclassified".
- test_dashboard_gets_code_and_lineage_for_every_level
    Purpose: the dashboard must be able to show a failed attempt too, and must not show a stale one.
    Method: Needs-review job with generated.py + lineage.json; export; expect both copied next to the status table.
            Delete the sources, export again; expect the copies gone.
"""
from __future__ import annotations

import json
from decimal import Decimal
from pathlib import Path

import pytest
import yaml

from engine.export import SOLUTION_MARKER, ExportError, export_all, reconciliation_report, status_record
from engine.model import Column, Table, write_table

SCHEMAS = {"t": [Column("id", "int"), Column("amount", "decimal(10,2)")]}
GENERATED = "from pyspark.sql import functions as F\n\n\ndef run(spark, tables):\n    return tables['t']\n"


def make_project(tmp_path: Path, jobs: dict[str, dict]) -> dict[str, Path]:
    """Lay out a miniature project: jobs/, out/, datasets/base/ and an empty ui folder.
    建立迷你專案:jobs/、out/、datasets/base/ 與空的 ui 資料夾。
    """
    paths = {name: tmp_path / name for name in ("jobs", "out", "ui")}
    paths["base"] = tmp_path / "datasets" / "base"
    write_table(Table(SCHEMAS["t"], [(1, Decimal("10.50"))]), paths["base"] / "t.csv")
    entries = []
    for job_id, spec in jobs.items():
        folder = paths["jobs"] / job_id
        folder.mkdir(parents=True)
        (folder / "legacy.sql").write_text("SELECT id, amount FROM t", encoding="utf-8")
        meta = {"job_id": job_id, "sources": [{"table_name": "t"}], "primary_keys": spec.get("keys", ["id"])}
        (folder / "meta.yaml").write_text(yaml.safe_dump(meta), encoding="utf-8")
        answer = Table([Column("id", "int"), Column("amount", "decimal(10,2)"), Column("ratio", "float")],
                       [(1, Decimal("10.50"), 0.5), (2, None, None)])
        write_table(answer, folder / "golden" / "base.csv")
        (paths["out"] / job_id).mkdir(parents=True)
        (paths["out"] / job_id / "generated.py").write_text(GENERATED, encoding="utf-8")
        entries.append({"id": job_id, "title": f"title {job_id}", "language": "tsql", "path": "ir",
                        "level": spec["level"], "reason": spec.get("reason", "")})
    (paths["out"] / "status.json").write_text(json.dumps({"jobs": entries}), encoding="utf-8")
    return paths


def run_export(paths: dict[str, Path]) -> dict:
    return export_all(paths["out"], paths["jobs"], paths["base"], paths["ui"], SCHEMAS)


def entry(level: str, path: str = "ir") -> dict:
    return {"id": "J1", "title": "t", "language": "tsql", "path": path, "level": level, "reason": ""}


# Purpose: a machine result must never arrive already approved by a human.
# 目的:機器產生的結果絕不能一出來就是「已人工核可」。
@pytest.mark.parametrize("level", ["Verified", "Tested", "Needs review"])
def test_approval_always_starts_unapproved(level):
    approval = status_record(entry(level), {})["human_approval"]
    assert approval["approved"] is False and approval["reviewer"] is None


# Purpose: Needs review may not be reported as reconciled.
# 目的:Needs review 不得被回報成對帳通過。
@pytest.mark.parametrize("level,expected", [("Verified", True), ("Tested", True), ("Needs review", False)])
def test_only_matching_levels_count_as_reconciled(level, expected):
    assert status_record(entry(level), {})["reconciliation_passed"] is expected


# Purpose: the headline numbers must equal what the job list says.
# 目的:總覽數字必須與 job 清單一致。
def test_metrics_add_up(tmp_path):
    paths = make_project(tmp_path, {"J1": {"level": "Verified"}, "J2": {"level": "Needs review"}})
    result = run_export(paths)
    assert result["store"]["metrics"] == {
        "total_jobs": 2, "verified": 1, "tested": 0, "needs_review": 1, "pass_rate_pct": 50.0,
    }
    saved = json.loads((paths["ui"] / "status_store.json").read_text(encoding="utf-8"))
    assert [j["job_id"] for j in saved["jobs"]] == ["J1", "J2"]


# Purpose: a Verified job is published for the CI gate with an exact answer and a runnable entry point.
# 目的:Verified 的 job 要發布給 CI 門禁,附上精確的答案檔與可執行的進入點。
def test_verified_job_gets_a_gate_package(tmp_path):
    import pyarrow.parquet as pq

    paths = make_project(tmp_path, {"J1": {"level": "Verified"}})
    result = run_export(paths)
    folder = paths["jobs"] / "J1"

    assert result["gate_jobs"] == ["J1"] and result["problems"] == {}
    rows = pq.read_table(folder / "oracle_output.parquet").to_pylist()
    assert rows == [{"id": 1, "amount": Decimal("10.50"), "ratio": 0.5}, {"id": 2, "amount": None, "ratio": None}]

    solution = (folder / "solution.py").read_text(encoding="utf-8")
    namespace: dict = {"__file__": str(folder / "solution.py")}
    exec(compile(solution, "solution.py", "exec"), namespace)
    assert callable(namespace["run_job"]) and callable(namespace["run"])
    with pytest.raises(RuntimeError, match="needs a SparkSession"):
        namespace["run_job"](None)

    columns = {c["name"]: c for c in yaml.safe_load((folder / "meta.yaml").read_text(encoding="utf-8"))["columns"]}
    assert columns["amount"] == {"name": "amount", "type": "DECIMAL(10,2)", "compare_mode": "exact"}
    assert columns["ratio"]["compare_mode"] == "tolerance" and columns["ratio"]["abs_tolerance"] == 1.0e-9


# Purpose: code that no longer matches legacy must not stay in front of the CI gate.
# 目的:已不再與舊系統一致的程式不得留在 CI 門禁前。
def test_needs_review_job_is_withdrawn_from_the_gate(tmp_path):
    paths = make_project(tmp_path, {"J1": {"level": "Needs review"}})
    folder = paths["jobs"] / "J1"
    (folder / "solution.py").write_text(f"{SOLUTION_MARKER}\n", encoding="utf-8")
    (folder / "oracle_output.parquet").write_bytes(b"stale")

    result = run_export(paths)

    assert result["gate_jobs"] == []
    assert not (folder / "solution.py").exists() and not (folder / "oracle_output.parquet").exists()


# Purpose: export must not destroy a teammate's hand-written solution.
# 目的:匯出不得刪掉隊友手寫的 solution。
def test_hand_written_solution_is_never_deleted(tmp_path):
    paths = make_project(tmp_path, {"J1": {"level": "Needs review"}})
    folder = paths["jobs"] / "J1"
    (folder / "solution.py").write_text("def run_job(spark):\n    return None\n", encoding="utf-8")
    (folder / "oracle_output.parquet").write_bytes(b"theirs")

    run_export(paths)

    assert (folder / "solution.py").exists() and (folder / "oracle_output.parquet").read_bytes() == b"theirs"


# Purpose: the CI gate fails packages without a key, so such a job is reported instead of published.
# 目的:CI 門禁會讓沒有主鍵的套件失敗,所以這種 job 要回報而不是發布。
def test_job_without_primary_key_is_reported_not_published(tmp_path):
    paths = make_project(tmp_path, {"J1": {"level": "Verified", "keys": []}})
    result = run_export(paths)

    assert "no primary key" in result["problems"]["J1"]
    assert not (paths["jobs"] / "J1" / "solution.py").exists()
    assert result["store"]["jobs"][0]["status"] == "Verified"


# Purpose: exporting before running must fail loudly instead of writing an empty status table.
# 目的:還沒執行就匯出必須明確失敗,而不是寫出空的狀態表。
def test_missing_run_results_is_an_error(tmp_path):
    with pytest.raises(ExportError, match="run-all"):
        export_all(tmp_path / "out", tmp_path / "jobs", tmp_path / "base", tmp_path / "ui", SCHEMAS)
    assert not (tmp_path / "ui" / "status_store.json").exists()


# Purpose: the report's summary must show the failure, not a dataset that happened to pass.
# 目的:報告摘要必須呈現失敗的那組資料,而不是剛好通過的那組。
def test_report_focuses_on_the_failing_dataset():
    def dataset(passed, missing):
        return {
            "passed": passed, "legacy_status": "ok",
            "checks": {
                "row_count": {"legacy": 6, "new": 6 - missing, "ok": not missing},
                "totals": {"amount": {"legacy": "40", "new": "36" if missing else "40", "ok": not missing}},
                "rows": {"missing": missing, "extra": 0, "different": 0, "ok": not missing},
            },
            "samples": {"missing": [{"row": {"id": i}, "times": 1} for i in range(missing)], "extra": [], "different": []},
        }

    engine_report = {
        "attempts": [{"source": "translate", "passed": False,
                      "datasets": {"base": dataset(True, 0), "adversarial": dataset(False, 5)}}],
        "counterexample": {"dataset": "adversarial", "shrunk_from_rows": 40, "shrunk_to_rows": 2, "reruns": 7},
    }
    report = reconciliation_report(entry("Needs review", "llm"), engine_report, "2026-11-12T00:00:00+00:00")

    assert report["dataset"] == "adversarial" and report["overall_passed"] is False
    assert report["summary"] == {"legacy_rows": 6, "spark_rows": 1, "count_diff": -5, "mismatched_rows": 5}
    assert report["field_checks"][0]["status"] == "FAILED"
    assert len(report["counterexamples"]) == 3 and report["counterexamples"][0]["kind"] == "missing_in_spark"
    assert report["shrinking_metadata"]["shrunk_input_rows"] == 2


# Purpose: published reasons must not carry absolute paths of the machine that ran the jobs.
# 目的:對外發布的原因文字不得帶有執行機器的絕對路徑。
def test_machine_paths_are_not_published(tmp_path):
    absolute = tmp_path.resolve() / "jobs" / "J1" / "golden"
    paths = make_project(tmp_path, {"J1": {"level": "Needs review", "reason": f"no answer file in {absolute}"}})
    reason = run_export(paths)["store"]["jobs"][0]["reason"]
    assert str(tmp_path.resolve()) not in reason and reason.endswith(str(Path("jobs") / "J1" / "golden"))


# Purpose: the status table shows the job's declared category, and says so when none is declared.
# 目的:狀態表顯示 job 宣告的分類;沒有宣告時要明說。
def test_category_comes_from_meta(tmp_path):
    paths = make_project(tmp_path, {"J1": {"level": "Verified"}, "J2": {"level": "Verified"}})
    meta_path = paths["jobs"] / "J1" / "meta.yaml"
    meta_path.write_text(meta_path.read_text(encoding="utf-8") + "category: Cleansing\n", encoding="utf-8")
    categories = {j["job_id"]: j["category"] for j in run_export(paths)["store"]["jobs"]}
    assert categories == {"J1": "Cleansing", "J2": "Unclassified"}


# Purpose: the dashboard must be able to show a failed attempt too, and must not show a stale one.
# 目的:儀表板也要能顯示失敗的嘗試,且不得顯示過期的版本。
def test_dashboard_gets_code_and_lineage_for_every_level(tmp_path):
    paths = make_project(tmp_path, {"J1": {"level": "Needs review"}})
    lineage = paths["out"] / "J1" / "lineage.json"
    lineage.write_text(json.dumps({"job": "J1", "tables": ["t"]}), encoding="utf-8")
    run_export(paths)
    assert (paths["ui"] / "code" / "J1.py").read_text(encoding="utf-8") == GENERATED
    assert json.loads((paths["ui"] / "lineage" / "J1.json").read_text(encoding="utf-8"))["tables"] == ["t"]

    lineage.unlink()
    (paths["out"] / "J1" / "generated.py").unlink()
    run_export(paths)
    assert not (paths["ui"] / "code" / "J1.py").exists() and not (paths["ui"] / "lineage" / "J1.json").exists()
