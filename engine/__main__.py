"""Command line entry point: `python -m engine <command>`.
命令列進入點:`python -m engine <指令>`。

Commands / 指令:
  make-base   sample the raw freMTPL2 CSVs into datasets/base      取樣原始 CSV
  run         process one job folder or .sql/.sas file             處理單一 job
  run-all     process every job and write out/status.json          處理全部 job
  export      publish results in the team's interface formats      依團隊介面格式匯出結果
  approve     record a human sign-off for a Verified/Tested job     記錄人工簽核
  revoke      withdraw a sign-off                                   撤回簽核

Settings come from environment variables or a `.env` file in the project root:
設定來自環境變數,或專案根目錄的 `.env` 檔:
  MF_SQLSERVER_CONN   ODBC connection string for the legacy SQL Server
  MF_OLLAMA_MODEL     model name (default qwen2.5-coder:3b)
  MF_OLLAMA_HOST      Ollama address (default http://localhost:11434)
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

from engine.approval import ApprovalError, approve, revoke
from engine.datasets import load_base, make_base
from engine.export import ExportError, export_all
from engine.job import load_schemas
from engine.legacy.runner import LegacyUnavailable, SqlServerRunner
from engine.llm.client import OllamaClient
from engine.pipeline import Context, run_all, run_job

ROOT = Path(__file__).resolve().parent.parent


def main(argv: list[str] | None = None) -> int:
    _load_env_file(ROOT / ".env")
    parser = argparse.ArgumentParser(prog="python -m engine", description="Legacy-to-PySpark migration factory")
    commands = parser.add_subparsers(dest="command", required=True)

    base = commands.add_parser("make-base", help="sample the raw freMTPL2 CSV files")
    base.add_argument("--data-dir", type=Path, default=ROOT.parent, help="folder holding freMTPL2freq.csv / freMTPL2sev.csv")
    base.add_argument("--full", action="store_true", help="keep every row instead of a sample (slow)")

    for name in ("run", "run-all"):
        sub = commands.add_parser(name, help="process one job" if name == "run" else "process every job")
        if name == "run":
            sub.add_argument("job", type=Path, help="job folder or .sql/.sas file")
        else:
            sub.add_argument("--only", nargs="*", help="job ids to process")
        sub.add_argument("--jobs", type=Path, default=ROOT / "jobs", help="jobs folder (holds _schemas.yaml)")
        sub.add_argument("--out", type=Path, default=ROOT / "out")
        sub.add_argument("--legacy", choices=("auto", "golden", "sqlserver"), default="auto",
                         help="auto: SQL Server when reachable, otherwise answer files")
        sub.add_argument("--record-golden", action="store_true", help="save SQL Server outputs as answer files")
        sub.add_argument("--llm", choices=("ollama", "off"), default="ollama")
        sub.add_argument("--no-base", action="store_true", help="skip the real-data sample")
        sub.add_argument("--no-shrink", action="store_true", help="do not shrink counterexamples")

    publish = commands.add_parser("export", help="write status_store.json, reports and CI gate packages")
    publish.add_argument("--jobs", type=Path, default=ROOT / "jobs")
    publish.add_argument("--out", type=Path, default=ROOT / "out")
    publish.add_argument("--ui", type=Path, default=ROOT / "ui" / "mock_data", help="folder for status_store.json")

    for name in ("approve", "revoke"):
        sign = commands.add_parser(name, help="record a human sign-off" if name == "approve" else "withdraw a sign-off")
        sign.add_argument("job_id")
        sign.add_argument("--jobs", type=Path, default=ROOT / "jobs")
        sign.add_argument("--ui", type=Path, default=ROOT / "ui" / "mock_data", help="folder holding status_store.json")
        if name == "approve":
            sign.add_argument("--reviewer", required=True, help="name of the person who read the code and the report")
            sign.add_argument("--notes", default="")

    args = parser.parse_args(argv)
    if args.command in ("approve", "revoke"):
        store_path = args.ui / "status_store.json"
        try:
            if args.command == "approve":
                result = approve(args.jobs, store_path, args.job_id, args.reviewer, args.notes)
                print(f"{args.job_id}: approved by {result['reviewer']} at {result['timestamp']}")
            else:
                existed = revoke(args.jobs, store_path, args.job_id)
                print(f"{args.job_id}: approval withdrawn" if existed else f"{args.job_id}: had no approval")
        except ApprovalError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        return 0

    schemas = load_schemas((getattr(args, "jobs", ROOT / "jobs")) / "_schemas.yaml")

    if args.command == "make-base":
        counts = make_base(args.data_dir, ROOT / "datasets" / "base", schemas, sample=not args.full)
        print(f"base dataset written: {counts}")
        return 0

    if args.command == "export":
        try:
            result = export_all(args.out, args.jobs, ROOT / "datasets" / "base", args.ui, schemas)
        except ExportError as exc:
            print(f"error: {exc}", file=sys.stderr)
            return 2
        print("metrics:", result["store"]["metrics"])
        print("CI gate packages:", ", ".join(result["gate_jobs"]) or "none")
        for job_id, reason in result["problems"].items():
            print(f"error: {job_id} not exported: {reason}", file=sys.stderr)
        return 1 if result["problems"] else 0

    ctx = Context(schemas=schemas, out_dir=args.out, record_golden=args.record_golden)
    if not args.no_base:
        ctx.base = load_base(ROOT / "datasets" / "base")
        if ctx.base is None:
            print("note: no base dataset (run `python -m engine make-base`); using made-up data only")
    if args.no_shrink:
        ctx.shrink_trials = 0
    if args.legacy != "golden":
        connection = os.environ.get("MF_SQLSERVER_CONN")
        try:
            if not connection:
                raise LegacyUnavailable("MF_SQLSERVER_CONN is not set")
            ctx.live = SqlServerRunner(connection)
            print("legacy oracle: SQL Server (live)")
        except LegacyUnavailable as exc:
            if args.legacy == "sqlserver":
                print(f"error: {exc}", file=sys.stderr)
                return 2
            print(f"legacy oracle: answer files only ({exc})")
    if args.llm == "ollama":
        ctx.llm = OllamaClient(
            model=os.environ.get("MF_OLLAMA_MODEL", "qwen2.5-coder:3b"),
            host=os.environ.get("MF_OLLAMA_HOST", "http://localhost:11434"),
        )

    if args.command == "run":
        entry = run_job(args.job, ctx)
        print(f"{entry['id']}: {entry['level']} ({entry['path'] or '-'}) {entry['reason']}")
        return 0
    status = run_all(args.jobs, ctx, args.only)
    print("summary:", status["summary"])
    return 0


def _load_env_file(path: Path) -> None:
    """Read KEY=VALUE lines into the environment without overriding existing variables.
    把 KEY=VALUE 各行讀進環境變數,不覆蓋已存在的變數。
    """
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line and not line.startswith("#") and "=" in line:
            key, _, value = line.partition("=")
            os.environ.setdefault(key.strip(), value.strip())


if __name__ == "__main__":
    sys.exit(main())
