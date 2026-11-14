"""Human sign-off for migrated jobs: record it, withdraw it, and decide whether it still holds.
已遷移 job 的人工簽核:記錄、撤回,以及判斷簽核是否仍然有效。

A sign-off is tied to the exact code that was reviewed. If the published `solution.py` changes, or the job
stops being Verified / Tested, the sign-off no longer counts and the job cannot reach the Gold layer.
簽核綁定當時審閱的那份程式。只要已發布的 `solution.py` 有變動,或 job 不再是 Verified / Tested,
簽核就不再算數,該 job 也無法進入 Gold 層。

Sign-offs live in `jobs/approvals.json` (committed), separate from the status table that `export` regenerates.
簽核存在 `jobs/approvals.json`(納入版控),與 `export` 會重新產生的狀態表分開。
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
from pathlib import Path

APPROVALS_FILE = "approvals.json"
PASSING_LEVELS = ("Verified", "Tested")


class ApprovalError(Exception):
    """The sign-off was refused; the message says why.
    簽核被拒絕;訊息會說明原因。
    """


def code_fingerprint(job_folder: Path) -> str | None:
    """SHA-256 of the published solution, or None when there is none.
    已發布 solution 的 SHA-256;沒有則回傳 None。

    Line endings are normalised first, so a Windows checkout and a Linux checkout agree.
    先統一換行符號,讓 Windows 與 Linux 的 checkout 算出相同結果。
    """
    solution = Path(job_folder) / "solution.py"
    if not solution.exists():
        return None
    text = solution.read_text(encoding="utf-8").replace("\r\n", "\n")
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def load_approvals(jobs_dir: Path) -> dict[str, dict]:
    path = Path(jobs_dir) / APPROVALS_FILE
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as error:
        raise ApprovalError(f"{path} is not valid JSON: {error}") from error
    if not isinstance(data, dict):
        raise ApprovalError(f"{path} must hold a JSON object keyed by job id")
    return data


def approval_for(jobs_dir: Path, job_id: str, level: str) -> dict:
    """The `human_approval` block for the status table, re-checked against the current level and code.
    狀態表用的 `human_approval` 區塊,會依目前等級與程式重新檢查。
    """
    record = load_approvals(jobs_dir).get(job_id)
    if record is None:
        return _not_approved("")
    if level not in PASSING_LEVELS:
        return _not_approved(f"approval by {record.get('reviewer')} withdrawn: job is now {level}")
    if code_fingerprint(Path(jobs_dir) / job_id) != record.get("code_sha256"):
        return _not_approved(f"approval by {record.get('reviewer')} withdrawn: code changed since the review")
    return {
        "approved": True,
        "reviewer": record.get("reviewer"),
        "timestamp": record.get("timestamp"),
        "notes": record.get("notes", ""),
    }


def approve(jobs_dir: Path, store_path: Path, job_id: str, reviewer: str, notes: str = "",
            now: dt.datetime | None = None) -> dict:
    """Record a reviewer's sign-off and reflect it in the status table.
    記錄審閱者的簽核,並反映到狀態表。

    Refused when the reviewer is blank, the job is unknown, the job is not Verified / Tested,
    or there is no published code to sign off.
    審閱者空白、job 不存在、job 不是 Verified / Tested,或沒有已發布的程式可簽核時,一律拒絕。
    """
    reviewer = (reviewer or "").strip()
    if not reviewer:
        raise ApprovalError("a reviewer name is required: an approval must be traceable to a person")
    store, job = _find_job(store_path, job_id)
    if job.get("status") not in PASSING_LEVELS:
        raise ApprovalError(f"{job_id} is {job.get('status')!r}; only Verified or Tested jobs can be approved")
    fingerprint = code_fingerprint(Path(jobs_dir) / job_id)
    if fingerprint is None:
        raise ApprovalError(f"{job_id} has no published solution.py to approve; run `python -m engine export`")

    stamp = (now or dt.datetime.now(dt.timezone.utc)).isoformat(timespec="seconds")
    approvals = load_approvals(jobs_dir)
    approvals[job_id] = {"reviewer": reviewer, "timestamp": stamp, "notes": notes, "code_sha256": fingerprint}
    _write_json(Path(jobs_dir) / APPROVALS_FILE, dict(sorted(approvals.items())))

    job["human_approval"] = approval_for(jobs_dir, job_id, job["status"])
    _write_json(store_path, store)
    return job["human_approval"]


def revoke(jobs_dir: Path, store_path: Path, job_id: str) -> bool:
    """Withdraw a sign-off. Returns False when there was none.
    撤回簽核。原本就沒有簽核時回傳 False。
    """
    store, job = _find_job(store_path, job_id)
    approvals = load_approvals(jobs_dir)
    existed = approvals.pop(job_id, None) is not None
    if existed:
        _write_json(Path(jobs_dir) / APPROVALS_FILE, approvals)
    job["human_approval"] = _not_approved("")
    _write_json(store_path, store)
    return existed


def _not_approved(notes: str) -> dict:
    return {"approved": False, "reviewer": None, "timestamp": None, "notes": notes}


def _find_job(store_path: Path, job_id: str) -> tuple[dict, dict]:
    store_path = Path(store_path)
    if not store_path.exists():
        raise ApprovalError(f"{store_path} not found; run `python -m engine export` first")
    store = json.loads(store_path.read_text(encoding="utf-8"))
    for job in store.get("jobs", []):
        if job.get("job_id") == job_id:
            return store, job
    raise ApprovalError(f"job {job_id!r} is not in {store_path}")


def _write_json(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
