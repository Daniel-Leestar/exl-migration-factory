"""Actions the dashboard can trigger: sign a job off, withdraw a sign-off, re-run a job.
儀表板可以觸發的動作:簽核 job、撤回簽核、重跑 job。

Kept apart from `ui/data.py` (read-only) and free of Streamlit so it can be tested on its own.
A re-run starts the same command a person would type (`python -m engine run ...` then `export`)
in a separate process, so Spark never lives inside the dashboard process.
與唯讀的 `ui/data.py` 分開,且不 import Streamlit,因此可以單獨測試。
重跑會在另一個行程執行與人手動輸入相同的指令(`python -m engine run ...` 再 `export`),
因此 Spark 不會跑在儀表板的行程裡。

Limit: there is no login. The reviewer name is whatever the person types; this suits a demo
or a trusted team, not an open network.
限制:沒有登入機制,審閱者姓名由使用者自行輸入;適用於 demo 或可信任的團隊,不適用於開放網路。
"""
from __future__ import annotations

import json
import os
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Callable

from engine.approval import ApprovalError, approve, revoke
from ui.data import UiDataError, _safe_id

DEFAULT_OLLAMA_HOST = "http://localhost:11434"
DEFAULT_OLLAMA_MODEL = "qwen2.5-coder:3b"
# A local 3b model on a laptop can need several minutes per answer, and a job may take four answers.
# 筆電上的 3b 本機模型每次回答可能要數分鐘,而一個 job 最多需要四次回答。
RERUN_TIMEOUT_SECONDS = 45 * 60
EXPORT_TIMEOUT_SECONDS = 10 * 60


class ActionError(Exception):
    """The requested action was refused or could not be completed.
    要求的動作被拒絕或無法完成。
    """


def settings(root: Path) -> dict:
    """Model address and name from the environment, else from `.env`, else the defaults.
    模型位址與名稱:先取環境變數,再取 `.env`,最後用預設值。
    """
    values = {}
    env_file = Path(root) / ".env"
    if env_file.exists():
        for line in env_file.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                values[key.strip()] = value.strip()
    values.update({k: v for k, v in os.environ.items() if k.startswith("MF_")})
    return {
        "ollama_host": values.get("MF_OLLAMA_HOST") or DEFAULT_OLLAMA_HOST,
        "ollama_model": values.get("MF_OLLAMA_MODEL") or DEFAULT_OLLAMA_MODEL,
    }


def model_status(host: str, model: str, opener: Callable = urllib.request.urlopen) -> tuple[bool, str]:
    """Whether the local model server answers and has the model. Returns (ready, message).
    本機模型伺服器是否有回應且已安裝該模型;回傳(是否就緒, 訊息)。

    Never raises: an unreachable server is an ordinary answer here, not an error.
    絕不丟例外:連不上伺服器在這裡是正常的回答,不是錯誤。
    """
    url = host.rstrip("/") + "/api/tags"
    try:
        with opener(url, timeout=3) as response:
            names = [m.get("name", "") for m in json.loads(response.read().decode("utf-8")).get("models", [])]
    except (urllib.error.URLError, TimeoutError, ConnectionError, OSError) as error:
        return False, f"cannot reach the model server at {host} ({error})"
    except (json.JSONDecodeError, AttributeError):
        return False, f"the server at {host} did not answer like an Ollama server"
    if model not in names:
        return False, f"the model server at {host} is running but has no model named {model}"
    return True, f"{model} is ready at {host}"


def sign_off(root: Path, job_id: str, reviewer: str, notes: str, confirmed: bool) -> dict:
    """Record a human sign-off. The reviewer must state that they read the code and the report.
    記錄人工簽核;審閱者必須聲明已閱讀程式與對帳報告。
    """
    if not confirmed:
        raise ActionError("tick the box to confirm you have read the code and the reconciliation report")
    try:
        return approve(Path(root) / "jobs", _store(root), _safe_id(job_id), reviewer, notes)
    except (ApprovalError, UiDataError) as error:
        raise ActionError(str(error)) from error


def withdraw(root: Path, job_id: str) -> bool:
    """Withdraw a sign-off. Returns False when there was none.
    撤回簽核;原本沒有簽核時回傳 False。
    """
    try:
        return revoke(Path(root) / "jobs", _store(root), _safe_id(job_id))
    except (ApprovalError, UiDataError) as error:
        raise ActionError(str(error)) from error


def rerun(root: Path, job_id: str, use_model: bool, runner: Callable = subprocess.run) -> str:
    """Run one job through the engine again and publish the result. Returns the command output.
    讓單一 job 重新走一次引擎並發布結果;回傳指令輸出。

    Not retried automatically: a failure here (bad job, engine error, time-out) will not fix itself.
    不自動重試:這裡的失敗(job 有問題、引擎錯誤、逾時)不會自己好。
    """
    root = Path(root)
    try:
        job_path = root / "jobs" / _safe_id(job_id)
    except UiDataError as error:
        raise ActionError(str(error)) from error
    if not job_path.is_dir():
        raise ActionError(f"job folder not found: {job_path}")
    steps = (
        ([sys.executable, "-m", "engine", "run", str(job_path), "--llm", "ollama" if use_model else "off"],
         RERUN_TIMEOUT_SECONDS),
        ([sys.executable, "-m", "engine", "export"], EXPORT_TIMEOUT_SECONDS),
    )
    output = []
    for command, timeout in steps:
        name = " ".join(command[2:4])
        try:
            done = runner(command, cwd=str(root), capture_output=True, text=True, timeout=timeout,
                          encoding="utf-8", errors="replace")
        except subprocess.TimeoutExpired as error:
            raise ActionError(f"`{name}` for {job_id} did not finish within {timeout // 60} minutes") from error
        except OSError as error:
            raise ActionError(f"`{name}` for {job_id} could not be started: {error}") from error
        text = ((done.stdout or "") + (done.stderr or "")).strip()
        output.append(f"$ python {' '.join(command[1:])}\n{text}")
        if done.returncode != 0:
            raise ActionError(f"`{name}` for {job_id} failed (exit code {done.returncode}):\n{text[-1500:]}")
    return "\n\n".join(output)


def _store(root: Path) -> Path:
    return Path(root) / "ui" / "mock_data" / "status_store.json"
