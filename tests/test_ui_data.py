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
    Method: both solution.py and the exported last attempt exist; expect solution.py content and its label.
- test_job_without_legacy_source_is_an_error
    Purpose: a job folder with nothing to migrate is reported, not rendered blank.
    Method: folder with only meta.yaml; expect UiDataError.
- test_lineage_falls_back_to_declared_tables
    Purpose: without a local run the dashboard still shows table-level lineage and says it is only declared.
    Method: no published lineage; expect tables from meta.yaml, confidence mentions "table level", DOT has table -> job.
- test_lineage_dot_draws_column_edges
    Purpose: derived lineage must be drawn column to column.
    Method: lineage with one output from two sources; expect both edges in the DOT text.
- test_job_id_cannot_leave_the_jobs_folder
    Purpose: a crafted job id must not read files outside the project.
    Method: job id "../x"; expect UiDataError from every loader.
- test_ai_log_is_read_in_order_and_empty_when_absent
    Purpose: the AI audit tab shows every model call, and says so plainly when there was none.
    Method: no file -> []; two JSON lines -> two calls in order; a broken line -> UiDataError with the line number.
- test_funnel_counts_each_stage
    Purpose: the progress funnel must agree with the Gold rule.
    Method: 4 jobs (verified+signed, verified, tested, needs review); expect 4 / 3 / 1.
- test_next_step_follows_status_and_sign_off
    Purpose: each job page tells the reviewer what to do next.
    Method: signed-off, Verified, Tested and blocked jobs; expect a different instruction for each.
- test_blocker_types_group_the_engine_reasons
    Purpose: analytics groups real engine reasons instead of listing free text.
    Method: one sample of each reason the engine writes; expect its group, and "Other" for unknown text.
- test_report_summary_states_the_verdict
    Purpose: the reconciliation tab opens with a plain verdict.
    Method: passing report, failing report, report with nothing compared, no report; expect the matching sentence.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from ui.data import (UiDataError, blocker_type, breakdown, funnel, gold_ready, job_rows, lineage_dot, load_ai_log,
                     load_job_files, load_lineage, load_report, load_store, next_step, report_summary)


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
    attempt = tmp_path / "ui" / "mock_data" / "code" / "J1.py"
    attempt.parent.mkdir(parents=True)
    attempt.write_text("last attempt", encoding="utf-8")
    files = load_job_files(tmp_path, "J1")
    assert files["generated"] == "last attempt" and "did not pass" in files["generated_origin"]

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


# Purpose: the AI audit tab shows every model call, and is empty when there was none.
# 目的:AI 稽核分頁顯示每一次模型呼叫;沒有呼叫時為空。
def test_ai_log_is_read_in_order_and_empty_when_absent(tmp_path):
    assert load_ai_log(tmp_path, "J1") == []
    log = tmp_path / "ai_log" / "J1.jsonl"
    log.parent.mkdir()
    log.write_text('{"purpose": "translate"}\n\n{"purpose": "repair 1"}\n', encoding="utf-8")
    assert [call["purpose"] for call in load_ai_log(tmp_path, "J1")] == ["translate", "repair 1"]
    log.write_text('{"purpose": "translate"}\nnot json\n', encoding="utf-8")
    with pytest.raises(UiDataError, match="line 2"):
        load_ai_log(tmp_path, "J1")


# Purpose: the progress funnel must agree with the Gold rule.
# 目的:進度漏斗必須與 Gold 規則一致。
def test_funnel_counts_each_stage():
    store = {"jobs": [job("A", "Verified", True), job("B", "Verified"), job("C", "Tested"), job("D", "Needs review", True)]}
    assert funnel(store) == {"jobs": 4, "reconciled": 3, "signed_off": 1}


# Purpose: each job page tells the reviewer what to do next.
# 目的:每個 job 頁面都告訴審閱者下一步。
def test_next_step_follows_status_and_sign_off():
    assert "promote" in next_step(job("A", "Verified", True))
    assert "fixed rules" in next_step(job("B", "Verified"))
    assert "AI audit" in next_step(job("C", "Tested"))
    blocked = job("D", "Needs review")
    blocked["reason"] = "no legacy output to compare against (unavailable)"
    assert next_step(blocked).startswith("Not ready: No legacy answer")


# Purpose: analytics groups real engine reasons instead of listing free text.
# 目的:分析頁把引擎真正寫出的原因分組,而不是列出自由文字。
@pytest.mark.parametrize("reason, group", [
    ("no legacy output to compare against (unavailable: no answer file)", "No legacy answer to compare against"),
    ("outside IR scope (MERGE statement) and the LLM path is switched off", "Needs the AI model, which was switched off"),
    ("still differs from legacy after 3 repair(s): 2 different rows", "AI translation did not match legacy"),
    ("the LLM could not produce a translation: no run()", "AI translation did not match legacy"),
    ("rule-generated code differs from legacy on 'base' data: totals differ", "Rule translation did not match legacy"),
    ("job could not be loaded: no legacy.sql", "Job could not be processed"),
    ("something new", "Other"),
    ("", "Other"),
])
def test_blocker_types_group_the_engine_reasons(reason, group):
    assert blocker_type(reason) == group


# Purpose: the reconciliation tab opens with a plain verdict.
# 目的:對帳分頁以白話結論開頭。
def test_report_summary_states_the_verdict():
    datasets = [{"name": "base", "passed": True}, {"name": "adversarial", "passed": True}, {"name": "empty", "passed": None}]
    passing = {"overall_passed": True, "datasets": datasets, "summary": {"legacy_rows": 21, "spark_rows": 21, "mismatched_rows": 0}}
    assert report_summary(passing) == "All 21 legacy rows match on the sample data, and 2 of 2 test datasets pass."

    failing = {"overall_passed": False, "datasets": [{"name": "base", "passed": False}],
               "summary": {"legacy_rows": 5, "spark_rows": 4, "mismatched_rows": 2}}
    assert "2 mismatched rows" in report_summary(failing) and "5 (legacy) vs 4 (Spark)" in report_summary(failing)

    assert "Nothing was compared" in report_summary({"datasets": [], "summary": {"legacy_rows": None}})
    assert "No reconciliation report" in report_summary(None)
