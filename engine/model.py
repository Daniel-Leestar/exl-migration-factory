"""Shared data shapes passed between every engine module.
所有引擎模組之間傳遞的共用資料形狀。

Type names are plain strings: int, bigint, float, decimal(p,s), string, bool, date.
型別名稱是單純字串:int、bigint、float、decimal(p,s)、string、bool、date。
"""
from __future__ import annotations

import csv
import datetime as dt
import hashlib
import re
from dataclasses import dataclass, field
from decimal import Decimal
from pathlib import Path

# Marker used for NULL in CSV files so that it differs from an empty string.
# CSV 檔中代表 NULL 的記號,用來和空字串區分。
NULL_TOKEN = "\\N"

_DECIMAL_RE = re.compile(r"^decimal\((\d+),\s*(\d+)\)$")


def is_decimal(type_name: str) -> bool:
    return type_name.startswith("decimal")


def is_integer(type_name: str) -> bool:
    return type_name in ("int", "bigint")


def is_numeric(type_name: str) -> bool:
    return is_integer(type_name) or is_decimal(type_name) or type_name == "float"


def decimal_parts(type_name: str) -> tuple[int, int]:
    """Return (precision, scale) of a decimal type name.
    回傳 decimal 型別的 (總位數, 小數位數)。
    """
    match = _DECIMAL_RE.match(type_name)
    if not match:
        raise ValueError(f"not a decimal type: {type_name!r}")
    return int(match.group(1)), int(match.group(2))


@dataclass(frozen=True)
class Column:
    name: str
    type: str


@dataclass
class Table:
    """A small in-memory table: column definitions plus rows of plain Python values.
    一張記憶體中的小表:欄位定義加上一列列的 Python 值。

    `synthetic` records whether the rows are made-up data. Only synthetic rows may
    ever be shown to an LLM.
    `synthetic` 記錄這些資料是否為合成資料;只有合成資料可以給 LLM 看。
    """

    columns: list[Column]
    rows: list[tuple] = field(default_factory=list)
    synthetic: bool = False

    @property
    def names(self) -> list[str]:
        return [c.name for c in self.columns]

    def with_rows(self, rows: list[tuple]) -> "Table":
        return Table(self.columns, rows, self.synthetic)


def parse_value(text: str, type_name: str):
    """Turn one CSV cell into a Python value of the given type.
    把一格 CSV 文字轉成指定型別的 Python 值。
    """
    if text == NULL_TOKEN:
        return None
    try:
        if is_integer(type_name):
            # Some exports write whole numbers as "12.0".
            # 有些匯出檔會把整數寫成 "12.0"。
            return int(text) if text.lstrip("-").isdigit() else int(float(text))
        if is_decimal(type_name):
            return Decimal(text)
        if type_name == "float":
            return float(text)
        if type_name == "bool":
            return text.strip().lower() in ("1", "true")
        if type_name == "date":
            return dt.date.fromisoformat(text)
    except (ValueError, ArithmeticError) as exc:
        raise ValueError(f"cannot read {text!r} as {type_name}") from exc
    return text


def format_value(value) -> str:
    if value is None:
        return NULL_TOKEN
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, float):
        return repr(value)
    return str(value)


def write_table(table: Table, path: Path) -> None:
    """Write a table as CSV whose header is `name:type` so the file describes itself.
    把表寫成 CSV,表頭為 `欄名:型別`,檔案本身就帶有結構資訊。
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow([f"{c.name}:{c.type}" for c in table.columns])
        for row in table.rows:
            writer.writerow([format_value(v) for v in row])


def read_table(path: Path, synthetic: bool = False) -> Table:
    """Read a CSV written by `write_table`.
    讀取 `write_table` 寫出的 CSV。
    """
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.reader(handle)
        header = next(reader, None)
        if not header:
            raise ValueError(f"{path}: file is empty, expected a 'name:type' header")
        columns = []
        for cell in header:
            name, sep, type_name = cell.partition(":")
            if not sep:
                raise ValueError(f"{path}: header cell {cell!r} is not 'name:type'")
            columns.append(Column(name, type_name))
        rows = [
            tuple(parse_value(cell, col.type) for cell, col in zip(line, columns))
            for line in reader
            if line
        ]
    return Table(columns, rows, synthetic)


def tables_fingerprint(tables: dict[str, Table]) -> str:
    """Hash input tables so a saved answer file can be tied to the exact inputs.
    對輸入表取雜湊,讓答案檔能對應到確切的輸入資料。
    """
    digest = hashlib.sha256()
    for name in sorted(tables):
        table = tables[name]
        digest.update(name.encode())
        digest.update(repr([(c.name, c.type) for c in table.columns]).encode())
        for row in table.rows:
            digest.update(repr(tuple(format_value(v) for v in row)).encode())
    return digest.hexdigest()
