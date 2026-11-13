"""Ways to obtain the legacy system's output for a job ("the oracle").
取得舊系統對某個 job 的輸出(標準答案)的幾種方式。

Two interchangeable runners:
- GoldenFileRunner reads answer files committed to the repo (fast; used in CI)
- SqlServerRunner really executes the legacy T-SQL on SQL Server (needed to re-run
  on new inputs, e.g. while shrinking a counterexample)
兩種可互換的執行器:
- GoldenFileRunner 讀取提交進 repo 的答案檔(快;CI 使用)
- SqlServerRunner 真的在 SQL Server 上執行舊 T-SQL(需要對新輸入重跑時使用,例如縮減反例)
"""
from __future__ import annotations

import datetime as dt
import re
from decimal import Decimal
from pathlib import Path

from engine.job import Job
from engine.model import Column, Table, is_decimal, read_table, tables_fingerprint, write_table

_GO_LINE = re.compile(r"^\s*GO\s*;?\s*$", re.IGNORECASE | re.MULTILINE)
_SQL_TYPES = {"int": "INT", "bigint": "BIGINT", "float": "FLOAT", "string": "VARCHAR(400)", "bool": "BIT", "date": "DATE"}


class LegacyError(Exception):
    """The legacy job itself failed on these inputs (for example a divide-by-zero).
    舊 job 本身在這組輸入上執行失敗(例如除以零)。
    """


class LegacyUnavailable(Exception):
    """There is no way to obtain the legacy output right now.
    目前沒有辦法取得舊系統的輸出。
    """


def golden_dir(job: Job) -> Path:
    parent = job.source_path.parent
    return parent / "golden" if job.source_path.stem.lower() == "legacy" else parent / "_golden" / job.id


class GoldenFileRunner:
    """Serve saved answers; refuses when the inputs differ from those the answer was made with.
    提供已存的答案;若輸入與當初產生答案時不同則拒絕。
    """

    can_rerun = False

    def run(self, job: Job, inputs: dict[str, Table], dataset: str) -> Table:
        folder = golden_dir(job)
        answer, stamp = folder / f"{dataset}.csv", folder / f"{dataset}.sha256"
        if not answer.exists() or not stamp.exists():
            raise LegacyUnavailable(f"no answer file for dataset {dataset!r} in {folder}")
        if stamp.read_text(encoding="utf-8").strip() != tables_fingerprint(inputs):
            raise LegacyUnavailable(f"answer file for {dataset!r} was made from different input data; record it again")
        table = read_table(answer)
        table.synthetic = all(t.synthetic for t in inputs.values())
        return table

    def save(self, job: Job, inputs: dict[str, Table], dataset: str, output: Table) -> None:
        folder = golden_dir(job)
        write_table(output, folder / f"{dataset}.csv")
        (folder / f"{dataset}.sha256").write_text(tables_fingerprint(inputs) + "\n", encoding="utf-8")


class SqlServerRunner:
    """Load the inputs into SQL Server, run the legacy script, read the result back.
    把輸入載入 SQL Server、執行舊腳本、再把結果讀回來。

    Works in its own database so nothing else on the server is touched.
    在專屬的資料庫中作業,不會動到伺服器上的其他東西。
    """

    can_rerun = True

    def __init__(self, connection_string: str, database: str = "mf_work", login_timeout: int = 30):
        import pyodbc

        if not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", database):
            raise ValueError(f"invalid database name: {database!r}")
        self._pyodbc = pyodbc
        try:
            try:
                self._conn = pyodbc.connect(connection_string, autocommit=True, timeout=login_timeout)
            except pyodbc.OperationalError:
                # Retryable: LocalDB goes to sleep when idle and its first login can time out while it wakes.
                # 可重試:LocalDB 閒置時會休眠,喚醒期間第一次登入可能逾時。
                self._conn = pyodbc.connect(connection_string, autocommit=True, timeout=login_timeout)
            cursor = self._conn.cursor()
            cursor.execute(f"IF DB_ID('{database}') IS NULL CREATE DATABASE [{database}]")
            cursor.execute(f"USE [{database}]")
        except pyodbc.Error as exc:
            raise LegacyUnavailable(f"cannot connect to SQL Server: {_message(exc)}") from exc

    def run(self, job: Job, inputs: dict[str, Table], dataset: str) -> Table:
        if job.language != "tsql":
            raise LegacyUnavailable(f"SQL Server cannot run {job.language} code")
        cursor = self._conn.cursor()
        try:
            for name, table in inputs.items():
                self._load(cursor, name, table)
            # A job may update one of its own inputs in place (e.g. a dimension table); keep that one.
            # job 可能就地更新自己的某張輸入表(例如維度表);這種情況不能刪。
            output_name = (job.output_table or "").split(".")[-1].strip("[]").lower()
            if job.output_table and output_name not in {name.lower() for name in inputs}:
                cursor.execute(f"DROP TABLE IF EXISTS {_quote_name(job.output_table)}")
        except self._pyodbc.Error as exc:
            raise LegacyUnavailable(f"could not load input tables into SQL Server: {_message(exc)}") from exc

        last = None
        try:
            for batch in _GO_LINE.split(job.legacy_code):
                if not batch.strip():
                    continue
                cursor.execute(batch)
                while True:
                    if cursor.description is not None:
                        last = (cursor.description, cursor.fetchall())
                    if not cursor.nextset():
                        break
            if job.output_table:
                cursor.execute(f"SELECT * FROM {_quote_name(job.output_table)}")
                last = (cursor.description, cursor.fetchall())
        except self._pyodbc.Error as exc:
            raise LegacyError(f"the legacy job failed on SQL Server: {_message(exc)}") from exc
        if last is None:
            raise LegacyError("the legacy job returned no result set and meta.yaml names no output table")

        description, rows = last
        columns = [Column(d[0], _type_from_description(d)) for d in description]
        if any(not c.name for c in columns):
            raise LegacyError("the legacy result has a column without a name")
        table = Table(columns, [tuple(row) for row in rows])
        table.synthetic = all(t.synthetic for t in inputs.values())
        return table

    def _load(self, cursor, name: str, table: Table) -> None:
        target = _quote_name(f"dbo.{name}")
        definitions = ", ".join(f"[{c.name}] {_sql_type(c.type)} NULL" for c in table.columns)
        cursor.execute(f"DROP TABLE IF EXISTS {target}")
        cursor.execute(f"CREATE TABLE {target} ({definitions})")
        if not table.rows:
            return
        statement = f"INSERT INTO {target} VALUES ({', '.join('?' * len(table.columns))})"
        # The bulk path is much faster but mis-guesses types when a column starts with NULLs,
        # so it is used only for large loads and falls back to the plain path on error.
        # 批次路徑快很多,但欄位開頭是 NULL 時會猜錯型別,因此只用於大量載入,失敗時退回一般路徑。
        if len(table.rows) > 2000:
            try:
                cursor.fast_executemany = True
                cursor.executemany(statement, table.rows)
                return
            except self._pyodbc.Error:
                cursor.execute(f"TRUNCATE TABLE {target}")
            finally:
                cursor.fast_executemany = False
        cursor.executemany(statement, table.rows)

    def close(self) -> None:
        self._conn.close()


def _quote_name(name: str) -> str:
    parts = [p.strip("[]") for p in name.split(".")]
    if not all(re.fullmatch(r"[#A-Za-z_][A-Za-z0-9_ ]*", p) for p in parts):
        raise LegacyUnavailable(f"unsafe table name: {name!r}")
    return ".".join(f"[{p}]" for p in parts)


def _sql_type(type_name: str) -> str:
    if is_decimal(type_name):
        return type_name.upper()
    if type_name not in _SQL_TYPES:
        raise LegacyUnavailable(f"no SQL Server type for {type_name!r}")
    return _SQL_TYPES[type_name]


def _type_from_description(description) -> str:
    type_code, precision, scale = description[1], description[4], description[5]
    if type_code is bool:
        return "bool"
    if type_code is int:
        return "bigint" if precision and precision > 10 else "int"
    if type_code is Decimal:
        return f"decimal({precision},{scale})"
    if type_code is float:
        return "float"
    if type_code in (dt.date, dt.datetime):
        return "date"
    return "string"


def _message(exc: Exception) -> str:
    text = str(exc.args[-1]) if exc.args else str(exc)
    return re.sub(r"\[[^\]]*\]", "", text).strip()[:300]
