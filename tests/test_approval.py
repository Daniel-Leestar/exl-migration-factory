"""Tests for engine/approval.py (human sign-off before a job may reach the Gold layer).
engine/approval.py 的測試(job 進入 Gold 層前的人工簽核)。

Test list / 測試清單
- test_approved_job_passes_the_gold_rule
    Purpose: a signed-off Verified job is recorded with who, when and for which code.
    Method: approve a Verified job; expect approved True, reviewer and timestamp kept in the status table
            and in jobs/approvals.json together with the code fingerprint.
- test_needs_review_job_cannot_be_approved
    Purpose: nobody can sign off code that does not match legacy.
    Method: approve a Needs-review job; expect ApprovalError and no approvals file.
- test_reviewer_name_is_required
    Purpose: an approval must be traceable to a person.
    Method: blank reviewer; expect ApprovalError.
- test_job_without_published_code_cannot_be_approved
    Purpose: there must be concrete code to sign off.
    Method: Verified job with no solution.py; expect ApprovalError mentioning export.
- test_unknown_job_is_refused
    Purpose: a typo in the job id must not create a stray approval.
    Method: approve "J99"; expect ApprovalError.
- test_approval_is_withdrawn_when_code_changes
    Purpose: a sign-off covers only the code that was reviewed.
    Method: approve, then edit solution.py; expect approval_for to return approved False with a "code changed" note.
- test_approval_is_withdrawn_when_job_stops_passing
    Purpose: a job that regresses to Needs review loses its sign-off.
    Method: approve, then ask approval_for with level Needs review; expect approved False.
- test_line_endings_do_not_break_an_approval
    Purpose: the same code checked out on Windows (CRLF) and Linux (LF) keeps its approval.
    Method: approve with LF, rewrite solution.py with CRLF; expect still approved.
- test_revoke_removes_the_approval
    Purpose: a reviewer can take a sign-off back.
    Method: approve then revoke; expect unapproved in the status table and gone from approvals.json;
            revoking again reports that there was nothing to revoke.
- test_export_keeps_a_valid_approval_and_drops_a_stale_one
    Purpose: regenerating the status table must neither lose a valid sign-off nor keep an outdated one.
    Method: export, approve, export again -> still approved; change the generated code, export -> not approved.
"""
from __future__ import annotations

import datetime as dt
import json
from pathlib import Path

import pytest

from engine.approval import ApprovalError, approval_for, approve, revoke
from tests.test_export import make_project, run_export

NOW = dt.datetime(2026, 11, 14, 12, 0, tzinfo=dt.timezone.utc)


def make_store(tmp_path: Path, status: str = "Verified", with_code: bool = True) -> tuple[Path, Path]:
    """A jobs folder with one job and a status table that lists it.
    含一個 job 的 jobs 資料夾,以及列出該 job 的狀態表。
    """
    jobs = tmp_path / "jobs"
    (jobs / "J1").mkdir(parents=True)
    if with_code:
        (jobs / "J1" / "solution.py").write_text("def run_job(spark):\n    return 1\n", encoding="utf-8", newline="\n")
    store = tmp_path / "ui" / "status_store.json"
    store.parent.mkdir(parents=True)
    job = {"job_id": "J1", "status": status,
           "human_approval": {"approved": False, "reviewer": None, "timestamp": None, "notes": ""}}
    store.write_text(json.dumps({"jobs": [job]}), encoding="utf-8")
    return jobs, store


def stored_approval(store: Path) -> dict:
    return json.loads(store.read_text(encoding="utf-8"))["jobs"][0]["human_approval"]


# Purpose: a signed-off Verified job is recorded with who, when and for which code.
# 目的:已簽核的 Verified job 要記錄是誰、何時、針對哪份程式。
def test_approved_job_passes_the_gold_rule(tmp_path):
    jobs, store = make_store(tmp_path)
    approve(jobs, store, "J1", " Pin-Yun ", "read code and report", now=NOW)

    assert stored_approval(store) == {"approved": True, "reviewer": "Pin-Yun",
                                      "timestamp": "2026-11-14T12:00:00+00:00", "notes": "read code and report"}
    saved = json.loads((jobs / "approvals.json").read_text(encoding="utf-8"))["J1"]
    assert saved["reviewer"] == "Pin-Yun" and len(saved["code_sha256"]) == 64


# Purpose: nobody can sign off code that does not match legacy.
# 目的:與舊系統不一致的程式,任何人都不能簽核。
def test_needs_review_job_cannot_be_approved(tmp_path):
    jobs, store = make_store(tmp_path, status="Needs review")
    with pytest.raises(ApprovalError, match="only Verified or Tested"):
        approve(jobs, store, "J1", "Pin-Yun")
    assert not (jobs / "approvals.json").exists() and stored_approval(store)["approved"] is False


# Purpose: an approval must be traceable to a person.
# 目的:核可必須能追溯到某個人。
@pytest.mark.parametrize("reviewer", ["", "   "])
def test_reviewer_name_is_required(tmp_path, reviewer):
    jobs, store = make_store(tmp_path)
    with pytest.raises(ApprovalError, match="reviewer name is required"):
        approve(jobs, store, "J1", reviewer)


# Purpose: there must be concrete code to sign off.
# 目的:必須有具體的程式可供簽核。
def test_job_without_published_code_cannot_be_approved(tmp_path):
    jobs, store = make_store(tmp_path, with_code=False)
    with pytest.raises(ApprovalError, match="engine export"):
        approve(jobs, store, "J1", "Pin-Yun")


# Purpose: a typo in the job id must not create a stray approval.
# 目的:job id 打錯不得產生多餘的核可記錄。
def test_unknown_job_is_refused(tmp_path):
    jobs, store = make_store(tmp_path)
    with pytest.raises(ApprovalError, match="J99"):
        approve(jobs, store, "J99", "Pin-Yun")
    assert not (jobs / "approvals.json").exists()


# Purpose: a sign-off covers only the code that was reviewed.
# 目的:簽核只涵蓋當時審閱的那份程式。
def test_approval_is_withdrawn_when_code_changes(tmp_path):
    jobs, store = make_store(tmp_path)
    approve(jobs, store, "J1", "Pin-Yun", now=NOW)
    (jobs / "J1" / "solution.py").write_text("def run_job(spark):\n    return 2\n", encoding="utf-8")

    result = approval_for(jobs, "J1", "Verified")
    assert result["approved"] is False and result["reviewer"] is None and "code changed" in result["notes"]


# Purpose: a job that regresses to Needs review loses its sign-off.
# 目的:退回 Needs review 的 job 會失去簽核。
def test_approval_is_withdrawn_when_job_stops_passing(tmp_path):
    jobs, store = make_store(tmp_path)
    approve(jobs, store, "J1", "Pin-Yun", now=NOW)
    result = approval_for(jobs, "J1", "Needs review")
    assert result["approved"] is False and "Needs review" in result["notes"]


# Purpose: the same code checked out on Windows and Linux keeps its approval.
# 目的:同一份程式在 Windows 與 Linux checkout 後,核可都要保持有效。
def test_line_endings_do_not_break_an_approval(tmp_path):
    jobs, store = make_store(tmp_path)
    approve(jobs, store, "J1", "Pin-Yun", now=NOW)
    (jobs / "J1" / "solution.py").write_bytes(b"def run_job(spark):\r\n    return 1\r\n")
    assert approval_for(jobs, "J1", "Verified")["approved"] is True


# Purpose: a reviewer can take a sign-off back.
# 目的:審閱者可以撤回簽核。
def test_revoke_removes_the_approval(tmp_path):
    jobs, store = make_store(tmp_path)
    approve(jobs, store, "J1", "Pin-Yun", now=NOW)

    assert revoke(jobs, store, "J1") is True
    assert stored_approval(store)["approved"] is False
    assert json.loads((jobs / "approvals.json").read_text(encoding="utf-8")) == {}
    assert revoke(jobs, store, "J1") is False


# Purpose: regenerating the status table must neither lose a valid sign-off nor keep an outdated one.
# 目的:重新產生狀態表時,不得弄丟有效的簽核,也不得保留過期的簽核。
def test_export_keeps_a_valid_approval_and_drops_a_stale_one(tmp_path):
    paths = make_project(tmp_path, {"J1": {"level": "Verified"}})
    run_export(paths)
    approve(paths["jobs"], paths["ui"] / "status_store.json", "J1", "Pin-Yun", now=NOW)

    assert run_export(paths)["store"]["jobs"][0]["human_approval"]["approved"] is True

    generated = paths["out"] / "J1" / "generated.py"
    generated.write_text(generated.read_text(encoding="utf-8") + "\n# changed after the review\n", encoding="utf-8")
    approval = run_export(paths)["store"]["jobs"][0]["human_approval"]
    assert approval["approved"] is False and "code changed" in approval["notes"]
