"""Tests for ui/actions.py (what the dashboard buttons do).
ui/actions.py 的測試(儀表板按鈕實際做的事)。

Test list / 測試清單
- test_sign_off_needs_the_confirmation_tick
    Purpose: nobody signs off by accident; the reviewer must state they read the code and report.
    Method: sign off without the tick; expect ActionError and the job still unapproved.
- test_sign_off_then_withdraw
    Purpose: a sign-off from the dashboard is the same record the Gold gate reads, and can be taken back.
    Method: sign off a Verified job; expect approved True with the reviewer; withdraw; expect approved False.
- test_sign_off_refuses_a_failing_job
    Purpose: the dashboard cannot be used to approve code that does not match legacy.
    Method: sign off a Needs-review job; expect ActionError mentioning the level rule.
- test_rerun_runs_the_engine_then_publishes
    Purpose: a re-run must publish its result, and must respect the "use the model" choice.
    Method: fake process runner; expect `engine run <job> --llm ollama` then `engine export`, in that order;
            with use_model False expect `--llm off`.
- test_rerun_stops_at_the_first_failure
    Purpose: a failed run must not be published as if it had worked.
    Method: runner returns exit code 2 for `run`; expect ActionError with the output, and no export call.
- test_rerun_reports_a_timeout
    Purpose: a hung model must end in a clear message, not a frozen page.
    Method: runner raises TimeoutExpired; expect ActionError mentioning minutes.
- test_rerun_refuses_unknown_or_crafted_job_ids
    Purpose: a crafted job id must not start a process on a path outside the jobs folder.
    Method: ids "../x" and "J99"; expect ActionError and no process started.
- test_model_status_messages
    Purpose: the page must say why a re-run with the model cannot start.
    Method: fake opener that is unreachable / lacks the model / has it; expect (False, reason) twice, then (True, ...).
- test_settings_prefer_environment_over_env_file
    Purpose: the model address is configurable without editing code.
    Method: `.env` sets host and model, environment overrides the host; expect environment host, file model.
"""
from __future__ import annotations

import io
import json
import subprocess
import urllib.error
from pathlib import Path
from types import SimpleNamespace

import pytest

from ui.actions import ActionError, model_status, rerun, settings, sign_off, withdraw


def make_root(tmp_path: Path, status: str = "Verified") -> Path:
    """A project root with one job, its published code and a status table.
    含一個 job、其已發布程式與狀態表的專案根目錄。
    """
    job = tmp_path / "jobs" / "J1"
    job.mkdir(parents=True)
    (job / "legacy.sql").write_text("SELECT 1 AS a FROM t", encoding="utf-8")
    (job / "solution.py").write_text("def run_job(spark):\n    return 1\n", encoding="utf-8")
    store = tmp_path / "ui" / "mock_data" / "status_store.json"
    store.parent.mkdir(parents=True)
    record = {"job_id": "J1", "status": status,
              "human_approval": {"approved": False, "reviewer": None, "timestamp": None, "notes": ""}}
    store.write_text(json.dumps({"jobs": [record]}), encoding="utf-8")
    return tmp_path


def approval(root: Path) -> dict:
    store = root / "ui" / "mock_data" / "status_store.json"
    return json.loads(store.read_text(encoding="utf-8"))["jobs"][0]["human_approval"]


class Runner:
    """Stand-in for subprocess.run that records commands and returns scripted exit codes.
    subprocess.run 的替身:記錄指令並回傳預先設定的結束碼。
    """

    def __init__(self, codes=(0, 0), error=None):
        self.commands, self._codes, self._error = [], list(codes), error

    def __call__(self, command, **options):
        self.commands.append(command)
        if self._error is not None:
            raise self._error
        return SimpleNamespace(returncode=self._codes.pop(0), stdout="out text", stderr="err text")


# Purpose: nobody signs off by accident.
# 目的:不會有人不小心簽核。
def test_sign_off_needs_the_confirmation_tick(tmp_path):
    root = make_root(tmp_path)
    with pytest.raises(ActionError, match="tick the box"):
        sign_off(root, "J1", "Pin-Yun", "", confirmed=False)
    assert approval(root)["approved"] is False


# Purpose: a dashboard sign-off is the record the Gold gate reads, and can be taken back.
# 目的:儀表板的簽核就是 Gold 門禁讀取的那筆紀錄,而且可以撤回。
def test_sign_off_then_withdraw(tmp_path):
    root = make_root(tmp_path)
    sign_off(root, "J1", "Pin-Yun", "read both", confirmed=True)
    assert (approval(root)["approved"], approval(root)["reviewer"]) == (True, "Pin-Yun")
    assert withdraw(root, "J1") is True and approval(root)["approved"] is False
    assert withdraw(root, "J1") is False


# Purpose: the dashboard cannot approve code that does not match legacy.
# 目的:儀表板不能用來核可與舊系統不一致的程式。
def test_sign_off_refuses_a_failing_job(tmp_path):
    root = make_root(tmp_path, status="Needs review")
    with pytest.raises(ActionError, match="only Verified or Tested"):
        sign_off(root, "J1", "Pin-Yun", "", confirmed=True)


# Purpose: a re-run publishes its result and respects the "use the model" choice.
# 目的:重跑會發布結果,並遵守「是否使用模型」的選擇。
def test_rerun_runs_the_engine_then_publishes(tmp_path):
    root, runner = make_root(tmp_path), Runner()
    output = rerun(root, "J1", use_model=True, runner=runner)
    run, export = runner.commands
    assert run[1:4] == ["-m", "engine", "run"] and run[4].endswith("J1") and run[-2:] == ["--llm", "ollama"]
    assert export[1:] == ["-m", "engine", "export"] and "out text" in output

    runner = Runner()
    rerun(root, "J1", use_model=False, runner=runner)
    assert runner.commands[0][-2:] == ["--llm", "off"]


# Purpose: a failed run must not be published as if it had worked.
# 目的:失敗的執行不得被當成成功而發布。
def test_rerun_stops_at_the_first_failure(tmp_path):
    root, runner = make_root(tmp_path), Runner(codes=(2,))
    with pytest.raises(ActionError, match="exit code 2") as caught:
        rerun(root, "J1", use_model=False, runner=runner)
    assert "err text" in str(caught.value) and len(runner.commands) == 1


# Purpose: a hung model ends in a clear message, not a frozen page.
# 目的:模型卡住時要給出清楚訊息,而不是讓頁面凍結。
def test_rerun_reports_a_timeout(tmp_path):
    runner = Runner(error=subprocess.TimeoutExpired(cmd="engine", timeout=1))
    with pytest.raises(ActionError, match="minutes"):
        rerun(make_root(tmp_path), "J1", use_model=True, runner=runner)


# Purpose: a crafted job id must not start a process outside the jobs folder.
# 目的:惡意構造的 job id 不得在 jobs 資料夾外啟動行程。
@pytest.mark.parametrize("job_id, message", [("../x", "invalid job id"), ("J99", "not found")])
def test_rerun_refuses_unknown_or_crafted_job_ids(tmp_path, job_id, message):
    runner = Runner()
    with pytest.raises(ActionError, match=message):
        rerun(make_root(tmp_path), job_id, use_model=False, runner=runner)
    assert runner.commands == []


# Purpose: the page must say why a re-run with the model cannot start.
# 目的:頁面要說明為何無法用模型重跑。
def test_model_status_messages():
    def unreachable(url, timeout):
        raise urllib.error.URLError("connection refused")

    def serving(names):
        return lambda url, timeout: io.BytesIO(json.dumps({"models": [{"name": n} for n in names]}).encode("utf-8"))

    ready, message = model_status("http://localhost:11434", "m1", opener=unreachable)
    assert ready is False and "cannot reach" in message
    ready, message = model_status("http://localhost:11434", "m1", opener=serving(["other"]))
    assert ready is False and "no model named m1" in message
    assert model_status("http://localhost:11434/", "m1", opener=serving(["m1"]))[0] is True


# Purpose: the model address is configurable without editing code.
# 目的:不改程式就能設定模型位址。
def test_settings_prefer_environment_over_env_file(tmp_path, monkeypatch):
    monkeypatch.delenv("MF_OLLAMA_HOST", raising=False)
    monkeypatch.delenv("MF_OLLAMA_MODEL", raising=False)
    assert settings(tmp_path) == {"ollama_host": "http://localhost:11434", "ollama_model": "qwen2.5-coder:3b"}

    (tmp_path / ".env").write_text("# comment\nMF_OLLAMA_HOST=http://file:1\nMF_OLLAMA_MODEL=file-model\n", encoding="utf-8")
    monkeypatch.setenv("MF_OLLAMA_HOST", "http://env:2")
    assert settings(tmp_path) == {"ollama_host": "http://env:2", "ollama_model": "file-model"}
