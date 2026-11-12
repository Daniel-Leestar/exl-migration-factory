"""Build small, made-up input tables designed to expose translation mistakes.
產生小型、虛構的輸入表,專門用來揪出翻譯錯誤。

Generic nasty values (NULL, negative numbers, repeated keys, mixed-case text,
trailing spaces) are always included. When the job has IR, the constants it
compares against are added too, together with the values just beside them,
so every boundary in a WHERE / CASE gets hit from both sides.
一律包含通用的刁鑽值(NULL、負數、重複鍵、大小寫混雜、尾端空白)。
job 有 IR 時,再加入它拿來比較的常數以及緊鄰的值,
讓 WHERE / CASE 的每個邊界兩側都被測到。

Everything is deterministic for a given seed and marked synthetic.
給定亂數種子時結果完全固定,且一律標記為合成資料。
"""
from __future__ import annotations

import datetime as dt
import random
import re
from decimal import Decimal

from engine import ir
from engine.ir_types import resolve_query
from engine.lineage import trace
from engine.model import Column, Table, decimal_parts, is_decimal, is_integer

_INTS = [0, 1, -1, 2, 3, 5, 7, 10, 50, 99, 100, 101, None]
_KEYS = [1, 2, 2, 3, 4, 5, 6, None]
_FLOATS = [0.0, 1.0, -1.5, 0.5, 0.005, 0.015, 2.675, 100.25, None]
_DECIMALS = ["0", "1", "-1.5", "0.01", "0.05", "99.99", "1000.10", "0.005", "2.675", None]
_TEXTS = ["A", "a", "A ", "B", "b", "Alpha", "ALPHA", "alpha ", "", "Zed", None]
_DATES = [dt.date(2020, 1, 1), dt.date(2020, 12, 31), dt.date(2021, 1, 1), dt.date(2024, 2, 29), None]


def empty_tables(schemas: dict[str, list[Column]]) -> dict[str, Table]:
    """Every input table with zero rows (an empty SUM must give NULL, COUNT must give 0).
    每張輸入表都是零列(空集合的 SUM 必須是 NULL、COUNT 必須是 0)。
    """
    return {name: Table(columns, [], synthetic=True) for name, columns in schemas.items()}


def generate_adversarial(
    schemas: dict[str, list[Column]],
    query: ir.Query | None = None,
    legacy_code: str = "",
    seed: int = 0,
    rows: int = 40,
) -> dict[str, Table]:
    """Return one adversarial table per input table.
    為每張輸入表回傳一張刁鑽資料表。
    """
    hints, divisors = _hints_from_query(query, schemas) if query is not None else ({}, set())
    divisor_names = {d.split(".")[1] for d in divisors} | _divisor_names_from_text(legacy_code)
    shared = _shared_names(schemas)
    rng = random.Random(seed)
    tables = {}
    for table_name in sorted(schemas):
        columns = schemas[table_name]
        domains = []
        for column in columns:
            domain = list(_base_domain(column, column.name.lower() in shared))
            domain += _near(hints.get(f"{table_name}.{column.name}".lower(), []), column)
            if column.name.lower() in divisor_names:
                # A zero divisor makes SQL Server abort the whole job, which would hide every other check.
                # 除數為零會讓 SQL Server 整個 job 中止,其他檢查就全看不到了。
                domain = [v for v in domain if v is None or v != 0]
            rng.shuffle(domain)
            domains.append(domain)
        data = []
        for i in range(rows):
            # The first rows walk through each column's values in turn so that none is left out.
            # 前幾列依序走過每個欄位的所有值,確保沒有值被漏掉。
            data.append(tuple(d[i % len(d)] if i < len(d) else rng.choice(d) for d in domains))
        tables[table_name] = Table(columns, data, synthetic=True)
    return tables


def _base_domain(column: Column, key_like: bool) -> list:
    if is_integer(column.type):
        return _KEYS if key_like else _INTS
    if column.type == "float":
        return _FLOATS
    if is_decimal(column.type):
        _, scale = decimal_parts(column.type)
        step = Decimal(1).scaleb(-scale)
        return [None if v is None else Decimal(v).quantize(step) for v in _DECIMALS]
    if column.type == "date":
        return _DATES
    if column.type == "bool":
        return [True, False, None]
    return _TEXTS


def _shared_names(schemas) -> set[str]:
    """Column names that look like join keys: present in several tables, or starting with "id".
    看起來像 join key 的欄位名:出現在多張表中,或以 "id" 開頭。
    """
    seen, shared = {}, set()
    for columns in schemas.values():
        for column in columns:
            name = column.name.lower()
            seen[name] = seen.get(name, 0) + 1
            if seen[name] > 1 or name.startswith("id"):
                shared.add(name)
    return shared


def _near(literals: list, column: Column) -> list:
    """The constants a job compares a column with, plus their immediate neighbours.
    job 拿來與欄位比較的常數,以及緊鄰的值。
    """
    values = []
    for literal in literals:
        if column.type == "string":
            text = str(literal)
            values += [text, text.upper(), text.lower(), text + " ", " " + text, text + "x"]
        elif is_integer(column.type):
            try:
                number = int(Decimal(str(literal)))
            except ArithmeticError:
                continue
            values += [number - 1, number, number + 1]
        elif column.type == "float":
            try:
                number = float(literal)
            except (TypeError, ValueError):
                continue
            values += [number - 0.001, number, number + 0.001]
        elif is_decimal(column.type):
            _, scale = decimal_parts(column.type)
            step = Decimal(1).scaleb(-scale)
            try:
                number = Decimal(str(literal)).quantize(step)
            except ArithmeticError:
                continue
            values += [number - step, number, number + step]
    return values


def _hints_from_query(query: ir.Query, schemas) -> tuple[dict[str, list], set[str]]:
    """Collect {table.column: constants it is compared with} and the columns used as divisors.
    收集 {表.欄位: 與之比較的常數},以及被當作除數的欄位。
    """
    hints: dict[str, list] = {}
    divisors: set[str] = set()

    def visit(q: ir.Query):
        for source in [q.source] + [j.source for j in q.joins]:
            if source.query is not None:
                visit(source.query)
        items = [o.expr for o in q.select] + [j.on for j in q.joins] + list(q.group_by)
        items += [c for c in (q.where, q.having) if c is not None]
        for item in items:
            for node in ir.walk(item):
                if isinstance(node, (ir.Cmp, ir.InList, ir.Like)):
                    literals = [n.value for n in ir.walk(node) if isinstance(n, ir.Lit) and n.type != "null"]
                    for base in trace(node, q):
                        hints.setdefault(base.lower(), []).extend(literals)
                elif isinstance(node, ir.BinOp) and node.op in ("/", "%"):
                    divisors.update(base.lower() for base in trace(node.right, q))

    try:
        visit(resolve_query(query, schemas))
    except ir.OutOfScope:
        # Hints are a bonus; generic adversarial data is still produced without them.
        # 提示只是加分項;沒有提示仍會產生通用的刁鑽資料。
        return {}, set()
    return hints, divisors


def _divisor_names_from_text(code: str) -> set[str]:
    """Rough guess at divisor columns for jobs without IR: any name right after a `/`.
    對沒有 IR 的 job 粗略猜測除數欄位:緊接在 `/` 後面的名稱。
    """
    return {m.split(".")[-1].strip("[]").lower() for m in re.findall(r"/\s*\(?\s*([A-Za-z_][\w.\[\]]*)", code or "")}
