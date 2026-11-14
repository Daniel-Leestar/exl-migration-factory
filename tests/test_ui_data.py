"""Tests for ui/data.py (what the dashboard reads and how it summarises it).
ui/data.py 的測試(儀表板讀什麼、怎麼彙總)。

Test list / 測試清單
- test_missing_status_table_is_an_error
    Purpose: the dashboard must say what to run instead of showing an empty table.
    Method: empty folder; expect UiDataError mentioning `engine export`.
- test_rows_keep_machine_result_and_human_approval_apart
    Purpose: "reconciled" and "approved by a human" are different facts and must not be merged.
    Method: Verified-but-unapproved job; expect Reconciled True, Human approved False.
- test_gold_ready_needs_both_reconciliation_and_approval
    Purpose: mirror the Gold deploy rule so the dashboard never promises more than the deploy allows.
    Method: 4 jobs (verified+approved, verified only, needs-review+approved, tested+approved); expect only 1st and 4th.
- test_breakdown_counts_by_dialect
    Purpose: analytics numbers must add up to the job list.
    Method: 2 T-SQL (Verified, Needs review) + 1 SAS (Needs review); expect exact counts per level.
- test_job_files_prefer_published_solution
    Purpose: show the code that passed reconciliation, not a later local attempt.
    Method: both solution.py and out/<job>/generated.py exist; expect solution.py content and its label.
- test_job_without_legacy_source_is_an_error
    Purpose: a job folder with nothing to migrate is reported, not rendered blank.
    Method: folder with only meta.yaml; expect UiDataError.
- test_lineage_falls_back_to_declared_tables
    Purpose: without a local run the dashboard still shows table-level lineage and says it is only declared.
    Method: no out/ folder; expect tables from meta.yaml, confidence mentions "table level", DOT has table -> job.
- test_lineage_dot_draws_column_edges
    Purpose: derived lineage must be drawn column to column.
    Method: lineage with one output from two sources; expect both edges in the DOT text.
- test_job_id_cannot_leave_the_jobs_folder
    Purpose: a crafted job id must not read files outside the project.
    Method: job id "../x"; expect UiDataError from every loader.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ui.data import (UiDataError, breakdown, gold_ready, job_rows, lineage_dot, load_job_files, load_lineage,
                     load_report, load_store)


def job(job_id, status, approved=False, dialect="T-SQL"):
    return {"job_id": job_id, "name": job_id, "dialect": dialect, "path": "IR", "status": status,
            "reconciliation_passed": status != "Needs review", "discrepancies": 0, "reason": "",
            "human_approval": {"approved": approved, "reviewer": None, "timestamp": None, "notes": ""}}


def make_job(root: Path, job_id: str = "J1") -> Path:
    folder = root / "jobs" / job_id
    folder.mkdir(parents=True)
    (folder / "legacy.sql").write_text("SELECT 1", encoding="utf-8")
    (folder / "meta.yaml").write_text(
        "job_id: J1\nsources:\n  - table_name: policy\n  - table_name: claims\ncolumns:\n  - name: Total\n", encoding="utf-8")
    return folder


# Purpose: the dashboard must say what to run instead of showing an empty table.
# 目的:儀表板要告訴使用者該執行什麼,而不是顯示空表。
def test_missing_status_table_is_an_error(tmp_path):
    with pytest.raises(UiDataError, match="engine export"):
        load_store(tmp_path)
    (tmp_path / "status_store.json").write_text(json.dumps({"jobs": "oops"}), encoding="utf-8")
    with pytest.raises(UiDataError, match="jobs"):
        load_store(tmp_path)


# Purpose: "reconciled" and "approved by a human" are different facts.
# 目的:「對帳通過」與「人工核可」是兩件事,不可混為一談。
def test_rows_keep_machine_result_and_human_approval_apart():
    row = job_rows({"jobs": [job("J1", "Verified")]})[0]
    assert row["Reconciled"] is True and row["Human approved"] is False


# Purpose: mirror the Gold deploy rule (passing level AND human approval).
# 目的:與 Gold 部署規則一致(等級通過「且」人工核可)。
def test_gold_ready_needs_both_reconciliation_and_approval():
    store = {"jobs": [job("A", "Verified", True), job("B", "Verified"), job("C", "Needs review", True), job("D", "Tested", True)]}
    assert gold_ready(store) == ["A", "D"]


# Purpose: analytics numbers must add up to the job list.
# 目的:分析數字必須與 job 清單吻合。
def test_breakdown_counts_by_dialect():
    store = {"jobs": [job("A", "Verified"), job("B", "Needs review"), job("C", "Needs review", dialect="SAS")]}
    assert breakdown(store, "dialect") == {
        "T-SQL": {"Verified": 1, "Tested": 0, "Needs review": 1},
        "SAS": {"Verified": 0, "Tested": 0, "Needs review": 1},
    }


# Purpose: show the code that passed reconciliation, not a later local attempt.
# 目的:顯示通過對帳的程式,而不是之後的本機嘗試。
def test_job_files_prefer_published_solution(tmp_path):
    folder = make_job(tmp_path)
    (tmp_path / "out" / "J1").mkdir(parents=True)
    (tmp_path / "out" / "J1" / "generated.py").write_text("local attempt", encoding="utf-8")
    files = load_job_files(tmp_path, "J1")
    assert files["generated"] == "local attempt" and "not published" in files["generated_origin"]

    (folder / "solution.py").write_text("published", encoding="utf-8")
    files = load_job_files(tmp_path, "J1")
    assert files["generated"] == "published" and files["legacy_language"] == "sql"
    assert files["meta"]["job_id"] == "J1"


# Purpose: a job folder with nothing to migrate is reported, not rendered blank.
# 目的:沒有可遷移程式的 job 資料夾要回報,而不是顯示空白。
def test_job_without_legacy_source_is_an_error(tmp_path):
    (tmp_path / "jobs" / "J9").mkdir(parents=True)
    with pytest.raises(UiDataError, match="legacy"):
        load_job_files(tmp_path, "J9")


# Purpose: without a local run, show table-level lineage and say it is only declared.
# 目的:沒有本機執行結果時,仍顯示表級 lineage,並標明只是宣告值。
def test_lineage_falls_back_to_declared_tables(tmp_path):
    make_job(tmp_path)
    meta = load_job_files(tmp_path, "J1")["meta"]
    lineage = load_lineage(tmp_path, "J1", meta)
    assert lineage["tables"] == ["policy", "claims"] and "table level" in lineage["confidence"]
    dot = lineage_dot(lineage, "J1")
    assert '"policy" -> "J1";' in dot and '"claims" -> "J1";' in dot


# Purpose: derived lineage must be drawn column to column.
# 目的:推導出的 lineage 必須畫成欄位對欄位。
def test_lineage_dot_draws_column_edges():
    lineage = {"tables": ["policy"], "outputs": {"Ratio": {"sources": ["policy.ClaimNb", "policy.Exposure"]}}}
    dot = lineage_dot(lineage, "J4")
    assert '"policy.ClaimNb" -> "J4.Ratio";' in dot and '"policy.Exposure" -> "J4.Ratio";' in dot
    assert '"policy" -> "J4";' not in dot


# Purpose: a crafted job id must not read files outside the project.
# 目的:惡意構造的 job id 不得讀到專案外的檔案。
def test_job_id_cannot_leave_the_jobs_folder(tmp_path):
    for loader in (lambda: load_report(tmp_path, "../x"), lambda: load_job_files(tmp_path, "../x"),
                   lambda: load_lineage(tmp_path, "../x", {})):
        with pytest.raises(UiDataError, match="invalid job id"):
            loader()
