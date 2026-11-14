"""The intermediate representation (IR): a language-neutral description of one query.
中間表示(IR):以與語言無關的方式描述一個查詢。

Everything is an immutable dataclass so that two equal pieces of logic compare equal,
and the whole tree converts to and from plain JSON.
所有節點都是不可變的 dataclass,相同的邏輯會被判定相等,整棵樹也能與 JSON 互轉。

IR v1 scope: read tables, filter, computed columns, INNER/LEFT JOIN,
GROUP BY with SUM/COUNT/AVG/MIN/MAX, HAVING.
IR v1 範圍:讀表、篩選、運算欄位、INNER/LEFT JOIN、
GROUP BY 搭配 SUM/COUNT/AVG/MIN/MAX、HAVING。
"""
from __future__ import annotations

import dataclasses
from dataclasses import dataclass
from decimal import Decimal

IR_VERSION = "1"


# ---------- expressions / 運算式 ----------

@dataclass(frozen=True)
class Col:
    """A column reference; `table` is the alias it belongs to (None until resolved).
    欄位參照;`table` 是所屬的別名(解析前為 None)。
    """
    name: str
    table: str | None = None


@dataclass(frozen=True)
class Lit:
    """A constant. `type` is int, decimal, string or null; decimals keep their text.
    常數。`type` 為 int、decimal、string 或 null;小數以文字保存。
    """
    value: object
    type: str


@dataclass(frozen=True)
class BinOp:
    """Arithmetic: + - * / %.
    四則與取餘數運算。
    """
    op: str
    left: object
    right: object


@dataclass(frozen=True)
class Cmp:
    """Comparison: = <> < <= > >=.
    比較運算。
    """
    op: str
    left: object
    right: object


@dataclass(frozen=True)
class Logic:
    """AND / OR over two or more conditions.
    兩個以上條件的 AND / OR。
    """
    op: str
    args: tuple


@dataclass(frozen=True)
class Not:
    arg: object


@dataclass(frozen=True)
class IsNull:
    arg: object
    negated: bool = False


@dataclass(frozen=True)
class InList:
    arg: object
    items: tuple
    negated: bool = False


@dataclass(frozen=True)
class Like:
    arg: object
    pattern: object
    negated: bool = False


@dataclass(frozen=True)
class Case:
    """CASE WHEN ... THEN ... ELSE ... END; `whens` holds (condition, value) pairs.
    CASE 運算;`whens` 存放 (條件, 值) 配對。
    """
    whens: tuple
    default: object | None = None


@dataclass(frozen=True)
class Func:
    """A scalar function from the supported list (see codegen).
    支援清單內的純量函數(見 codegen)。
    """
    name: str
    args: tuple


@dataclass(frozen=True)
class Cast:
    arg: object
    type: str


@dataclass(frozen=True)
class Agg:
    """An aggregate: SUM, COUNT, AVG, MIN, MAX. `arg` is None for COUNT(*).
    彙總函數;COUNT(*) 的 `arg` 為 None。
    """
    func: str
    arg: object | None = None
    distinct: bool = False


# ---------- query / 查詢 ----------

@dataclass(frozen=True)
class Source:
    """One input of a query: a named table or a nested query, with its alias.
    查詢的一個輸入:具名的表或巢狀查詢,以及它的別名。
    """
    alias: str
    table: str | None = None
    query: "Query | None" = None


@dataclass(frozen=True)
class Join:
    kind: str  # "inner" or "left"
    source: Source
    on: object


@dataclass(frozen=True)
class Output:
    name: str
    expr: object


@dataclass(frozen=True)
class Query:
    source: Source
    joins: tuple = ()
    where: object | None = None
    group_by: tuple = ()
    having: object | None = None
    select: tuple = ()


_NODE_TYPES = {
    cls.__name__: cls
    for cls in (Col, Lit, BinOp, Cmp, Logic, Not, IsNull, InList, Like, Case, Func, Cast, Agg, Source, Join, Output, Query)
}


def to_json(node):
    """Convert an IR tree to plain JSON data.
    把 IR 樹轉成單純的 JSON 資料。
    """
    if dataclasses.is_dataclass(node):
        data = {"node": type(node).__name__}
        for f in dataclasses.fields(node):
            data[f.name] = to_json(getattr(node, f.name))
        return data
    if isinstance(node, tuple):
        return [to_json(item) for item in node]
    if isinstance(node, Decimal):
        return str(node)
    return node


def from_json(data):
    """Rebuild an IR tree from `to_json` output.
    從 `to_json` 的輸出重建 IR 樹。
    """
    if isinstance(data, dict):
        kind = data.get("node")
        if kind not in _NODE_TYPES:
            raise ValueError(f"unknown IR node kind: {kind!r}")
        return _NODE_TYPES[kind](**{k: from_json(v) for k, v in data.items() if k != "node"})
    if isinstance(data, list):
        return tuple(from_json(item) for item in data)
    return data


def children(node) -> list:
    """Direct sub-expressions of an expression node.
    某個運算式節點底下的直接子運算式。
    """
    found = []
    if not dataclasses.is_dataclass(node) or isinstance(node, (Query, Source)):
        return found
    for f in dataclasses.fields(node):
        value = getattr(node, f.name)
        for item in value if isinstance(value, tuple) else (value,):
            if isinstance(item, tuple):
                found.extend(x for x in item if dataclasses.is_dataclass(x))
            elif dataclasses.is_dataclass(item):
                found.append(item)
    return found


def walk(node):
    """Yield an expression and everything beneath it (does not enter sub-queries).
    走訪運算式及其下所有節點(不進入子查詢)。
    """
    yield node
    for child in children(node):
        yield from walk(child)


class OutOfScope(Exception):
    """The legacy code uses something IR v1 does not cover; the job goes to the LLM path.
    舊程式用到 IR v1 未涵蓋的功能;該 job 改走 LLM 路徑。
    """

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def substitute(node, mapping: dict):
    """Replace whole sub-expressions found in `mapping`, largest match first.
    把出現在 `mapping` 中的整段子運算式替換掉,優先比對較大的運算式。
    """
    if isinstance(node, tuple):
        return tuple(substitute(item, mapping) for item in node)
    if not dataclasses.is_dataclass(node) or isinstance(node, (Query, Source)):
        return node
    if node in mapping:
        return mapping[node]
    changes = {f.name: substitute(getattr(node, f.name), mapping) for f in dataclasses.fields(node)}
    return dataclasses.replace(node, **changes)


def transform(node, fn):
    """Rebuild an expression bottom-up, passing every node through `fn`.
    由下而上重建運算式,每個節點都經過 `fn` 處理。
    """
    if isinstance(node, tuple):
        return tuple(transform(item, fn) for item in node)
    if not dataclasses.is_dataclass(node) or isinstance(node, (Query, Source)):
        return node
    changes = {f.name: transform(getattr(node, f.name), fn) for f in dataclasses.fields(node)}
    return fn(dataclasses.replace(node, **changes))


def show(node) -> str:
    """Readable SQL-like text for an expression, used in lineage and reports.
    把運算式轉成易讀的類 SQL 文字,用於 lineage 與報告。
    """
    if node is None:
        return ""
    if isinstance(node, Col):
        return f"{node.table}.{node.name}" if node.table else node.name
    if isinstance(node, Lit):
        if node.type == "null":
            return "NULL"
        return "'" + str(node.value).replace("'", "''") + "'" if node.type == "string" else str(node.value)
    if isinstance(node, (BinOp, Cmp)):
        return f"({show(node.left)} {node.op} {show(node.right)})"
    if isinstance(node, Logic):
        return "(" + f" {node.op.upper()} ".join(show(a) for a in node.args) + ")"
    if isinstance(node, Not):
        return f"NOT {show(node.arg)}"
    if isinstance(node, IsNull):
        return f"{show(node.arg)} IS {'NOT ' if node.negated else ''}NULL"
    if isinstance(node, InList):
        return f"{show(node.arg)} {'NOT ' if node.negated else ''}IN ({', '.join(show(i) for i in node.items)})"
    if isinstance(node, Like):
        return f"{show(node.arg)} {'NOT ' if node.negated else ''}LIKE {show(node.pattern)}"
    if isinstance(node, Case):
        parts = [f"WHEN {show(c)} THEN {show(v)}" for c, v in node.whens]
        if node.default is not None:
            parts.append(f"ELSE {show(node.default)}")
        return "CASE " + " ".join(parts) + " END"
    if isinstance(node, Func):
        return f"{node.name}({', '.join(show(a) for a in node.args)})"
    if isinstance(node, Cast):
        return f"CAST({show(node.arg)} AS {node.type})"
    if isinstance(node, Agg):
        inner = "*" if node.arg is None else ("DISTINCT " if node.distinct else "") + show(node.arg)
        return f"{node.func}({inner})"
    raise TypeError(f"cannot show {type(node).__name__}")
