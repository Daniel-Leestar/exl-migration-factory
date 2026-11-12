"""Load a legacy job (code + settings) from the jobs folder.
從 jobs 資料夾讀取一個舊 job(程式碼加設定)。

A job is either a folder holding `legacy.sql` / `legacy.sas` plus `meta.yaml`,
or a bare `.sql` / `.sas` file with no settings at all (the "unknown job" case).
一個 job 可以是含 `legacy.sql` / `legacy.sas` 與 `meta.yaml` 的資料夾,
也可以是完全沒有設定的單一 `.sql` / `.sas` 檔(「未知 job」情境)。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import yaml

from engine.model import Column

LANGUAGE_BY_SUFFIX = {".sql": "tsql", ".sas": "sas"}


class JobError(Exception):
    """The job folder or its settings cannot be used.
    job 資料夾或其設定無法使用。
    """


@dataclass
class Job:
    id: str
    language: str  # "tsql" or "sas"
    legacy_code: str
    source_path: Path
    title: str = ""
    trap: str = ""
    # Input table names. Empty means "not declared": every known table is offered.
    # 輸入表名稱;空的代表「未宣告」,此時提供所有已知的表。
    inputs: list[str] = field(default_factory=list)
    primary_key: list[str] = field(default_factory=list)
    # Legacy table holding the result; None means the script's last result set.
    # 舊系統存放結果的表;None 代表取腳本最後一個查詢結果。
    output_table: str | None = None
    # Text columns compared without regard to upper/lower case.
    # 比對時不分大小寫的文字欄位。
    ci_columns: list[str] = field(default_factory=list)


def load_schemas(path: Path) -> dict[str, list[Column]]:
    """Read the shared table definitions: {table: {column: type}}.
    讀取共用的表結構定義:{表: {欄位: 型別}}。
    """
    if not path.exists():
        raise JobError(f"schema file not found: {path}")
    raw = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(raw, dict):
        raise JobError(f"{path}: expected a mapping of table -> columns")
    schemas = {}
    for table, columns in raw.items():
        if not isinstance(columns, dict) or not columns:
            raise JobError(f"{path}: table {table!r} must map column names to types")
        schemas[str(table)] = [Column(str(n), str(t)) for n, t in columns.items()]
    return schemas


def load_job(path: Path) -> Job:
    """Load a job from a folder or from a single code file.
    從資料夾或單一程式檔載入 job。
    """
    path = Path(path)
    if path.is_file():
        return _from_file(path, job_id=path.stem, meta={})
    if not path.is_dir():
        raise JobError(f"job path does not exist: {path}")

    code_files = sorted(p for p in path.iterdir() if p.name.lower() in ("legacy.sql", "legacy.sas"))
    if not code_files:
        raise JobError(f"{path}: no legacy.sql or legacy.sas found")
    meta_path = path / "meta.yaml"
    meta = {}
    if meta_path.exists():
        try:
            meta = yaml.safe_load(meta_path.read_text(encoding="utf-8")) or {}
        except yaml.YAMLError as exc:
            raise JobError(f"{meta_path}: not valid YAML ({exc})") from exc
        if not isinstance(meta, dict):
            raise JobError(f"{meta_path}: expected a mapping at the top level")
    return _from_file(code_files[0], job_id=str(meta.get("id") or path.name), meta=meta)


def _from_file(code_path: Path, job_id: str, meta: dict) -> Job:
    language = LANGUAGE_BY_SUFFIX.get(code_path.suffix.lower())
    if language is None:
        raise JobError(f"{code_path}: unsupported file type, expected .sql or .sas")
    output = meta.get("output") or {}
    return Job(
        id=job_id,
        language=str(meta.get("language") or language),
        legacy_code=code_path.read_text(encoding="utf-8-sig"),
        source_path=code_path,
        title=str(meta.get("title") or ""),
        trap=str(meta.get("trap") or ""),
        inputs=[str(t) for t in (meta.get("inputs") or [])],
        primary_key=[str(c) for c in (output.get("primary_key") or [])],
        output_table=output.get("table"),
        ci_columns=[str(c) for c in (output.get("ci_columns") or [])],
    )


def discover_jobs(jobs_dir: Path) -> list[Path]:
    """List job folders and bare code files, skipping names starting with `_`.
    列出 job 資料夾與單一程式檔,略過以 `_` 開頭的名稱。
    """
    found = []
    for entry in sorted(Path(jobs_dir).iterdir()):
        if entry.name.startswith(("_", ".")):
            continue
        if entry.is_dir() or entry.suffix.lower() in LANGUAGE_BY_SUFFIX:
            found.append(entry)
    return found
