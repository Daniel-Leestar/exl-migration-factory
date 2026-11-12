"""Name resolution and type inference for the IR, following SQL Server's rules.
依 SQL Server 的規則,為 IR 做欄位名稱解析與型別推斷。

Types decide the traps: `int / int` truncates and `AVG(int)` truncates only because
both sides are integers, so the generator must know every expression's type.
型別決定陷阱:`int / int` 與 `AVG(int)` 之所以會截斷,是因為兩邊都是整數,
因此程式產生器必須知道每個運算式的型別。
"""
from __future__ import annotations

from dataclasses import replace
from decimal import Decimal

from engine import ir
from engine.ir import OutOfScope
from engine.model import Column, decimal_parts, is_decimal, is_integer

# Supported scalar functions: name -> (min args, max args).
# 支援的純量函數:名稱 -> (最少參數, 最多參數)。
FUNCTIONS = {
    "UPPER": (1, 1), "LOWER": (1, 1), "LTRIM": (1, 1), "RTRIM": (1, 1), "TRIM": (1, 1),
    "LEN": (1, 1), "SUBSTRING": (3, 3), "LEFT": (2, 2), "RIGHT": (2, 2), "REPLACE": (3, 3),
    "CONCAT": (2, 99), "COALESCE": (2, 99), "ISNULL": (2, 2), "NULLIF": (2, 2),
    "ABS": (1, 1), "ROUND": (2, 2), "TRUNCATE": (2, 2),
}
_TEXT_FUNCTIONS = {"UPPER", "LOWER", "LTRIM", "RTRIM", "TRIM", "SUBSTRING", "LEFT", "RIGHT", "REPLACE", "CONCAT"}
AGGREGATES = {"SUM", "COUNT", "AVG", "MIN", "MAX"}


class Scope:
    """The columns visible at one point of a query: alias -> columns.
    查詢中某個位置看得到的欄位:別名 -> 欄位清單。
    """

    def __init__(self):
        self.entries: list[tuple[str, list[Column]]] = []

    def add(self, alias: str, columns: list[Column]) -> None:
        if any(a.lower() == alias.lower() for a, _ in self.entries):
            raise OutOfScope(f"alias {alias!r} is used twice in the same query")
        self.entries.append((alias, columns))

    def find(self, col: ir.Col) -> tuple[str, Column]:
        """Return (alias, column) for a reference; names match ignoring case.
        回傳欄位參照對應的 (別名, 欄位);名稱比對不分大小寫。
        """
        matches = [
            (alias, c)
            for alias, columns in self.entries
            if col.table is None or alias.lower() == col.table.lower()
            for c in columns
            if c.name.lower() == col.name.lower()
        ]
        if not matches:
            raise OutOfScope(f"unknown column {ir.show(col)!r}")
        if len(matches) > 1:
            raise OutOfScope(f"column {ir.show(col)!r} is ambiguous; qualify it with a table alias")
        return matches[0]


def source_columns(source: ir.Source, catalog: dict[str, list[Column]]) -> list[Column]:
    if source.query is not None:
        return output_columns(source.query, catalog)
    for name, columns in catalog.items():
        if name.lower() == source.table.lower():
            return columns
    raise OutOfScope(f"unknown table {source.table!r}; it is not in the schema file")


def query_scope(query: ir.Query, catalog: dict[str, list[Column]]) -> Scope:
    scope = Scope()
    scope.add(query.source.alias, source_columns(query.source, catalog))
    for join in query.joins:
        scope.add(join.source.alias, source_columns(join.source, catalog))
    return scope


def resolve_query(query: ir.Query, catalog: dict[str, list[Column]]) -> ir.Query:
    """Qualify every column with its alias and canonical name, in nested queries too.
    把每個欄位補上所屬別名與標準名稱,巢狀查詢也一併處理。
    """

    def resolve_source(source: ir.Source) -> ir.Source:
        if source.query is not None:
            return replace(source, query=resolve_query(source.query, catalog))
        for name in catalog:
            if name.lower() == source.table.lower():
                return replace(source, table=name)
        raise OutOfScope(f"unknown table {source.table!r}; it is not in the schema file")

    query = replace(
        query,
        source=resolve_source(query.source),
        joins=tuple(replace(j, source=resolve_source(j.source)) for j in query.joins),
    )
    scope = query_scope(query, catalog)

    def qualify(node):
        if isinstance(node, ir.Col):
            alias, column = scope.find(node)
            return ir.Col(column.name, alias)
        return node

    names = [o.name.lower() for o in query.select]
    if len(set(names)) != len(names):
        raise OutOfScope("two output columns share the same name")
    for clause, label in ((query.where, "WHERE"), (query.group_by, "GROUP BY")):
        if any(isinstance(n, ir.Agg) for item in _as_tuple(clause) for n in ir.walk(item)):
            raise OutOfScope(f"aggregate function inside {label}")

    return replace(
        query,
        joins=tuple(replace(j, on=ir.transform(j.on, qualify)) for j in query.joins),
        where=ir.transform(query.where, qualify),
        group_by=ir.transform(query.group_by, qualify),
        having=ir.transform(query.having, qualify),
        select=tuple(replace(o, expr=ir.transform(o.expr, qualify)) for o in query.select),
    )


def output_columns(query: ir.Query, catalog: dict[str, list[Column]]) -> list[Column]:
    """Names and types of the columns a query returns.
    查詢回傳的欄位名稱與型別。
    """
    scope = query_scope(query, catalog)
    return [Column(o.name, _or_int(infer(o.expr, scope))) for o in query.select]


def infer(node, scope: Scope) -> str:
    """Type of an expression. Returns "null" for a bare NULL and "bool" for conditions.
    運算式的型別。單獨的 NULL 回傳 "null",條件式回傳 "bool"。
    """
    if isinstance(node, ir.Col):
        return scope.find(node)[1].type
    if isinstance(node, ir.Lit):
        if node.type == "int":
            return "int" if -2**31 <= int(node.value) < 2**31 else "bigint"
        if node.type == "decimal":
            return literal_decimal_type(str(node.value))
        return node.type
    if isinstance(node, ir.BinOp):
        return _arithmetic(node.op, infer(node.left, scope), infer(node.right, scope), node)
    if isinstance(node, (ir.Cmp, ir.Logic, ir.Not, ir.IsNull, ir.InList, ir.Like)):
        return "bool"
    if isinstance(node, ir.Case):
        values = [v for _, v in node.whens] + ([node.default] if node.default is not None else [])
        return unify([infer(v, scope) for v in values], node)
    if isinstance(node, ir.Cast):
        return node.type
    if isinstance(node, ir.Func):
        return _function(node, scope)
    if isinstance(node, ir.Agg):
        return _aggregate(node, scope)
    raise OutOfScope(f"cannot type {type(node).__name__}")


def literal_decimal_type(text: str) -> str:
    digits = Decimal(text).as_tuple()
    scale = max(-digits.exponent, 0)
    return f"decimal({max(len(digits.digits), scale, 1)},{scale})"


def unify(types: list[str], node=None) -> str:
    """The single type several branches (CASE, COALESCE) settle on.
    多個分支(CASE、COALESCE)最後統一成的型別。
    """
    types = [t for t in types if t != "null"]
    if not types:
        return "null"
    if all(t == types[0] for t in types):
        return types[0]
    if any(t == "string" for t in types) or any(t in ("date", "bool") for t in types):
        raise OutOfScope(f"branches mix incompatible types {sorted(set(types))} in {ir.show(node) if node else '?'}")
    if "float" in types:
        return "float"
    if any(is_decimal(t) for t in types):
        parts = [decimal_parts(_as_decimal(t)) for t in types]
        scale = max(s for _, s in parts)
        return decimal_type(max(p - s for p, s in parts) + scale, scale)
    return "bigint" if "bigint" in types else "int"


def decimal_type(precision: int, scale: int) -> str:
    """Apply SQL Server's cap of 38 digits, giving up scale (but keeping at least 6) first.
    套用 SQL Server 的 38 位上限,優先犧牲小數位(但至少保留 6 位)。
    """
    if precision > 38:
        scale = max(38 - (precision - scale), min(scale, 6))
        precision = 38
    return f"decimal({precision},{scale})"


def _as_decimal(type_name: str) -> str:
    return {"int": "decimal(10,0)", "bigint": "decimal(19,0)"}.get(type_name, type_name)


def operand_decimal(node, type_name: str) -> str:
    """Decimal type an operand takes inside decimal arithmetic.
    運算元在小數運算中被當成的 decimal 型別。

    SQL Server sizes a whole-number constant by its digits (3 -> decimal(1,0)) but an
    int column as decimal(10,0); the difference changes the scale of a division result.
    SQL Server 依位數決定整數常數的大小(3 -> decimal(1,0)),但 int 欄位是 decimal(10,0);
    這個差別會改變除法結果的小數位數。
    """
    if isinstance(node, ir.Lit) and node.type == "int":
        return f"decimal({len(str(abs(int(node.value))))},0)"
    return _as_decimal(type_name)


def _or_int(type_name: str) -> str:
    # SQL Server gives an untyped NULL column the type int.
    # SQL Server 會把沒有型別的 NULL 欄位當成 int。
    return "int" if type_name == "null" else type_name


def _arithmetic(op: str, left: str, right: str, node) -> str:
    if left == "null" or right == "null":
        other = right if left == "null" else left
        return _or_int(other)
    if left == "string" and right == "string" and op == "+":
        return "string"
    if "string" in (left, right) or "bool" in (left, right) or "date" in (left, right):
        raise OutOfScope(f"arithmetic on non-numeric types ({left} {op} {right}) in {ir.show(node)}")
    if "float" in (left, right):
        return "float"
    if is_decimal(left) or is_decimal(right):
        (p1, s1), (p2, s2) = decimal_parts(operand_decimal(node.left, left)), decimal_parts(operand_decimal(node.right, right))
        if op in ("+", "-"):
            scale = max(s1, s2)
            return decimal_type(max(p1 - s1, p2 - s2) + scale + 1, scale)
        if op == "*":
            return decimal_type(p1 + p2 + 1, s1 + s2)
        if op == "/":
            scale = max(6, s1 + p2 + 1)
            return decimal_type(p1 - s1 + s2 + scale, scale)
        scale = max(s1, s2)
        return decimal_type(min(p1 - s1, p2 - s2) + scale, scale)
    return "bigint" if "bigint" in (left, right) else "int"


def _function(node: ir.Func, scope: Scope) -> str:
    low, high = FUNCTIONS[node.name]
    if not low <= len(node.args) <= high:
        raise OutOfScope(f"{node.name} called with {len(node.args)} argument(s)")
    arg_types = [infer(a, scope) for a in node.args]
    if node.name in _TEXT_FUNCTIONS:
        return "string"
    if node.name == "LEN":
        return "int"
    if node.name == "COALESCE":
        return unify(arg_types, node)
    if node.name == "ISNULL":
        # ISNULL keeps the type of its first argument, unlike COALESCE.
        # ISNULL 會沿用第一個參數的型別,和 COALESCE 不同。
        unify(arg_types, node)
        return arg_types[0] if arg_types[0] != "null" else arg_types[1]
    if node.name in ("ROUND", "TRUNCATE"):
        if arg_types[0] == "float" and node.name == "TRUNCATE":
            raise OutOfScope("ROUND(x, n, 1) truncation on a FLOAT value has no exact Spark equivalent")
        if not isinstance(node.args[1], ir.Lit) or node.args[1].type != "int":
            raise OutOfScope("ROUND needs a constant whole number of digits")
    return _or_int(arg_types[0])


def _aggregate(node: ir.Agg, scope: Scope) -> str:
    if node.func == "COUNT":
        return "int"
    if node.arg is None:
        raise OutOfScope(f"{node.func}(*) is not valid")
    if any(isinstance(n, ir.Agg) for n in ir.walk(node.arg)):
        raise OutOfScope("aggregate inside another aggregate")
    arg = _or_int(infer(node.arg, scope))
    if node.func in ("MIN", "MAX"):
        return arg
    if arg in ("string", "bool", "date"):
        raise OutOfScope(f"{node.func} over a {arg} value")
    if arg == "float" or is_integer(arg):
        return arg
    _, scale = decimal_parts(arg)
    return f"decimal(38,{scale})" if node.func == "SUM" else f"decimal(38,{max(scale, 6)})"


def _as_tuple(value) -> tuple:
    if value is None:
        return ()
    return value if isinstance(value, tuple) else (value,)
