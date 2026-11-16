"""Settings shared by the command line, the dashboard and the end-to-end runner.
命令列、儀表板與端到端執行器共用的設定。

Every setting is an environment variable; a `.env` file in the project root supplies defaults
without overriding variables that are already set. See `.env.example` for the full list.
每個設定都是環境變數;專案根目錄的 `.env` 提供預設值,但不覆蓋已存在的變數。完整清單見 `.env.example`。
"""
from __future__ import annotations

import os
from pathlib import Path

DEFAULT_OLLAMA_HOST = "http://localhost:11434"
DEFAULT_OLLAMA_MODEL = "qwen2.5-coder:3b"
DEFAULT_LAKEHOUSE_ROOT = "data"


def load_env_file(path: Path) -> None:
    """Read KEY=VALUE lines into the environment without overriding existing variables.
    把 KEY=VALUE 各行讀進環境變數,不覆蓋已存在的變數。
    """
    path = Path(path)
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


def ollama_host() -> str:
    return os.environ.get("MF_OLLAMA_HOST") or DEFAULT_OLLAMA_HOST


def ollama_model() -> str:
    return os.environ.get("MF_OLLAMA_MODEL") or DEFAULT_OLLAMA_MODEL


def lakehouse_root() -> str:
    """Folder that holds the raw / bronze / silver / quarantine / gold layers.
    存放 raw / bronze / silver / quarantine / gold 各層的資料夾。

    A local folder by default. Point it at a mounted cloud folder to write there instead.
    預設為本機資料夾;指向已掛載的雲端資料夾即可改寫到那裡。
    """
    return os.environ.get("MF_LAKEHOUSE_ROOT") or DEFAULT_LAKEHOUSE_ROOT
