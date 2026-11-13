"""Turn legacy T-SQL into IR, or say clearly why it falls outside IR v1.
把舊 T-SQL 轉成 IR;超出 IR v1 範圍時明確說明原因。

sqlglot does the syntax work. This module is deliberately strict: anything it does
not recognise raises OutOfScope instead of being guessed at, because a wrong guess
here would be labelled "deterministic" later.
語法解析交給 sqlglot。本模組刻意從嚴:任何不認得的寫法都丟出 OutOfScope 而不猜測,
因為這裡猜錯的結果之後會被標成「確定性」。
"""
from __future__ import annotations

import re

import sqlglot
from sqlglot import exp
from sqlglot.errors import SqlglotError

from engine import ir
from engine.ir import OutOfScope
from engine.ir_types import FUNCTIONS

_GO_LINE = re.compile(r"^\s*GO\s*;?\s*$", re.IGNORECASE | re.MULTILINE)

# Statement kinds that set things up but do not change the result.
# 只做環境設定、不影響結果的陳述式種類。
_IGNORABLE = (exp.Set, exp.Use, exp.Drop)

_STATEMENT_REASONS = {
    "Merge": "MERGE statement",
    "Create": "CREATE statement (stored procedure, table or other DDL)",
    "Update": "UPDATE statement",
    "Delete": "DELETE statement",
    "Union": "UNION / set operation",
    "Command": "procedural T-SQL (variables, control flow or EXEC)",
    "If": "procedural T-SQL (IF block)",
    "While": "procedural T-SQL (WHILE loop)",
    "Declare": "procedural T-SQL (DECLARE)",
}

_BINARY = {exp.Add: "+", exp.Sub: "-", exp.Mul: "*", exp.Div: "/", exp.Mod: "%"}
_COMPARE = {exp.EQ: "=", exp.NEQ: "<>", exp.GT: ">", exp.GTE: ">=", exp.LT: "<", exp.LTE: "<="}
_AGGREGATES = {exp.Sum: "SUM", exp.Avg: "AVG", exp.Min: "MIN", exp.Max: "MAX", exp.Count: "COUNT"}
_SIMPLE_FUNCTIONS = {exp.Upper: "UPPER", exp.Lower: "LOWER", exp.Abs: "ABS"}
_TEXT_TYPES = {"VARCHAR", "NVARCHAR", "TEXT"}
_NUMBER_TYPES = {"INT": "int", "BIGINT": "bigint", "FLOAT": "float", "DOUBLE": "float"}


def parse_tsql(code: str) -> ir.Query:
    """Parse one legacy T-SQL job into an IR query.
    把一個舊 T-SQL job 解析成 IR 查詢。

    Raises OutOfScope with a human-readable reason for anything unsupported.
    遇到不支援的內容會丟出 OutOfScope,並附上人看得懂的原因。
    """
    try:
        statements = [s for s in sqlglot.parse(_GO_LINE.sub("", code), read="tsql") if s is not None]
    except SqlglotError as exc:
        raise OutOfScope(f"the SQL parser could not read this code: {str(exc).splitlines()[0][:200]}") from exc
    except RecursionError as exc:
        raise OutOfScope("the SQL is nested too deeply to parse") from exc

    work = [s for s in statements if not isinstance(s, _IGNORABLE)]
    if not work:
        raise OutOfScope("no query found in the file")
    for statement in work:
        kind = type(statement).__name__
        if kind in _STATEMENT_REASONS:
            raise OutOfScope(_STATEMENT_REASONS[kind])
        # Look inside as well, so the reason names the real construct rather than its wrapper.
        # 也往內層找,讓原因指出真正的語法,而不是包在外面的那一層。
        for node_type, reason in ((exp.Merge, "MERGE statement"), (exp.Window, "window function (OVER ...)")):
            if statement.find(node_type) is not None:
                raise OutOfScope(reason)
    if len(work) > 1:
        raise OutOfScope(f"{len(work)} statements in one job (multi-step script); IR v1 handles a single query")

    statement = work[0]
    if isinstance(statement, exp.Insert):
        _only(statement, {"this", "expression"}, "INSERT")
        statement = statement.expression
    if not isinstance(statement, exp.Select):
        raise OutOfScope(f"unsupported statement type {type(statement).__name__}")
    return _query(statement, cte_env={}, top_level=True)


def _only(node: exp.Expression, allowed: set, label: str) -> None:
    """Refuse a node that carries any argument this module does not handle.
    若節點帶有本模組未處理的參數,直接拒絕。
    """
    for key, value in node.args.items():
        if key not in allowed and value not in (None, [], False):
            raise OutOfScope(f"{label} uses an unsupported feature ({key.rstrip('_').upper()})")


def _query(select: exp.Select, cte_env: dict, top_level: bool = False) -> ir.Query:
    allowed = {"expressions", "from_", "joins", "where", "group", "having", "with_"}
    if top_level:
        # ORDER BY at the top only affects row order, which reconciliation ignores; INTO only names the target.
        # 最外層的 ORDER BY 只影響列順序(對帳不比順序);INTO 只是指定輸出位置。
        allowed |= {"order", "into"}
    _only(select, allowed, "SELECT")

    cte_env = dict(cte_env)
    with_ = select.args.get("with_")
    if with_ is not None:
        _only(with_, {"expressions"}, "WITH")
        for cte in with_.expressions:
            if not isinstance(cte.this, exp.Select):
                raise OutOfScope("a CTE that is not a plain SELECT")
            cte_env[cte.alias.lower()] = _query(cte.this, cte_env)

    from_ = select.args.get("from_")
    if from_ is None:
        raise OutOfScope("SELECT without FROM")
    source = _source(from_.this, cte_env)

    joins = []
    for join in select.args.get("joins") or []:
        _only(join, {"this", "side", "kind", "on"}, "JOIN")
        side, kind = (join.args.get("side") or "").upper(), (join.args.get("kind") or "").upper()
        if side == "LEFT" and kind in ("", "OUTER"):
            join_kind = "left"
        elif side == "" and kind in ("", "INNER"):
            join_kind = "inner"
        else:
            raise OutOfScope(f"{(side + ' ' + kind).strip()} JOIN; IR v1 supports INNER and LEFT joins")
        if join.args.get("on") is None:
            raise OutOfScope("join without an ON condition (cross join)")
        joins.append(ir.Join(join_kind, _source(join.this, cte_env), _expr(join.args["on"])))

    group = select.args.get("group")
    if group is not None:
        _only(group, {"expressions"}, "GROUP BY")

    outputs = []
    for item in select.expressions:
        if isinstance(item, exp.Alias):
            outputs.append(ir.Output(item.alias, _expr(item.this)))
        elif isinstance(item, exp.Column) and not isinstance(item.this, exp.Star):
            outputs.append(ir.Output(item.name, _expr(item)))
        elif isinstance(item, exp.Star) or isinstance(getattr(item, "this", None), exp.Star):
            raise OutOfScope("SELECT * (columns must be listed so lineage is exact)")
        else:
            raise OutOfScope(f"an output column has no name: {item.sql(dialect='tsql')[:60]}")

    where, having = select.args.get("where"), select.args.get("having")
    return ir.Query(
        source=source,
        joins=tuple(joins),
        where=_expr(where.this) if where is not None else None,
        group_by=tuple(_expr(e) for e in group.expressions) if group is not None else (),
        having=_expr(having.this) if having is not None else None,
        select=tuple(outputs),
    )


def _source(node: exp.Expression, cte_env: dict) -> ir.Source:
    if isinstance(node, exp.Subquery):
        _only(node, {"this", "alias"}, "sub-query")
        if not isinstance(node.this, exp.Select):
            raise OutOfScope("a sub-query that is not a plain SELECT")
        if not node.alias:
            raise OutOfScope("sub-query without an alias")
        return ir.Source(alias=node.alias, query=_query(node.this, cte_env))
    if isinstance(node, exp.Table):
        _only(node, {"this", "db", "catalog", "alias"}, "table reference")
        if not isinstance(node.this, exp.Identifier):
            raise OutOfScope("table-valued function or other non-table source")
        name = node.name
        if not node.args.get("db") and name.lower() in cte_env:
            return ir.Source(alias=node.alias or name, query=cte_env[name.lower()])
        if name.startswith("#"):
            raise OutOfScope("reads a temporary table")
        return ir.Source(alias=node.alias or name, table=name)
    raise OutOfScope(f"unsupported source in FROM ({type(node).__name__})")


def _expr(node: exp.Expression):
    kind = type(node)

    if kind is exp.Paren:
        return _expr(node.this)
    if kind is exp.Column:
        _only(node, {"this", "table"}, "column reference")
        if not isinstance(node.this, exp.Identifier):
            raise OutOfScope("SELECT * (columns must be listed so lineage is exact)")
        return ir.Col(node.name, node.table or None)
    if kind is exp.Literal:
        return _literal(node)
    if kind is exp.National:
        return ir.Lit(node.name, "string")
    if kind is exp.Null:
        return ir.Lit(None, "null")
    if kind is exp.Neg:
        inner = _expr(node.this)
        if isinstance(inner, ir.Lit) and inner.type == "int":
            return ir.Lit(-inner.value, "int")
        if isinstance(inner, ir.Lit) and inner.type == "decimal":
            return ir.Lit("-" + inner.value, "decimal")
        return ir.BinOp("-", ir.Lit(0, "int"), inner)

    if kind in _BINARY:
        return ir.BinOp(_BINARY[kind], _expr(node.this), _expr(node.expression))
    if kind in _COMPARE:
        return ir.Cmp(_COMPARE[kind], _expr(node.this), _expr(node.expression))
    if kind in (exp.And, exp.Or):
        op = "and" if kind is exp.And else "or"
        args = []
        for side in (_expr(node.this), _expr(node.expression)):
            args.extend(side.args if isinstance(side, ir.Logic) and side.op == op else (side,))
        return ir.Logic(op, tuple(args))
    if kind is exp.Not:
        inner = _expr(node.this)
        if isinstance(inner, (ir.IsNull, ir.InList, ir.Like)) and not inner.negated:
            return type(inner)(**{**inner.__dict__, "negated": True})
        return ir.Not(inner)
    if kind is exp.Is:
        if not isinstance(node.expression, exp.Null):
            raise OutOfScope("IS comparison with something other than NULL")
        return ir.IsNull(_expr(node.this))
    if kind is exp.In:
        _only(node, {"this", "expressions"}, "IN")
        return ir.InList(_expr(node.this), tuple(_expr(e) for e in node.expressions))
    if kind is exp.Between:
        _only(node, {"this", "low", "high"}, "BETWEEN")
        value = _expr(node.this)
        return ir.Logic("and", (ir.Cmp(">=", value, _expr(node.args["low"])), ir.Cmp("<=", value, _expr(node.args["high"]))))
    if kind is exp.Like:
        _only(node, {"this", "expression"}, "LIKE")
        pattern = _expr(node.expression)
        if not isinstance(pattern, ir.Lit) or pattern.type != "string" or "[" in pattern.value:
            raise OutOfScope("LIKE with a non-constant pattern or a [character set]")
        return ir.Like(_expr(node.this), pattern)

    if kind is exp.Case:
        subject = _expr(node.this) if node.this is not None else None
        whens = []
        for branch in node.args["ifs"]:
            condition = _expr(branch.this)
            if subject is not None:
                condition = ir.Cmp("=", subject, condition)
            whens.append((condition, _expr(branch.args["true"])))
        default = node.args.get("default")
        return ir.Case(tuple(whens), _expr(default) if default is not None else None)
    if kind is exp.If:
        false = node.args.get("false")
        return ir.Case(((_expr(node.this), _expr(node.args["true"])),), _expr(false) if false is not None else None)

    if kind is exp.Cast:
        _only(node, {"this", "to"}, "CAST")
        return ir.Cast(_expr(node.this), _data_type(node.args["to"]))
    if kind in _AGGREGATES:
        return _aggregate(node, _AGGREGATES[kind])
    return _function(node)


def _literal(node: exp.Literal) -> ir.Lit:
    if node.is_string:
        return ir.Lit(node.this, "string")
    text = node.this
    if re.fullmatch(r"\d+", text):
        return ir.Lit(int(text), "int")
    if re.fullmatch(r"\d*\.\d*", text) and text != ".":
        return ir.Lit(text, "decimal")
    raise OutOfScope(f"numeric literal {text!r} (floating-point notation)")


def _data_type(node: exp.DataType) -> str:
    name = node.this.name
    params = [p.this.name if isinstance(p, exp.DataTypeParam) else p.name for p in node.expressions]
    if name in _NUMBER_TYPES and not params:
        return _NUMBER_TYPES[name]
    if name == "DECIMAL":
        precision = int(params[0]) if params else 18
        scale = int(params[1]) if len(params) > 1 else 0
        return f"decimal({precision},{scale})"
    if name in _TEXT_TYPES:
        return "string"
    raise OutOfScope(f"CAST to {node.sql(dialect='tsql')} is not supported")


def _aggregate(node: exp.Expression, name: str) -> ir.Agg:
    _only(node, {"this", "big_int"}, name)
    arg = node.this
    distinct = False
    if isinstance(arg, exp.Distinct):
        if len(arg.expressions) != 1:
            raise OutOfScope(f"{name}(DISTINCT ...) over several columns")
        arg, distinct = arg.expressions[0], True
    if isinstance(arg, exp.Star):
        if name != "COUNT":
            raise OutOfScope(f"{name}(*) is not valid")
        return ir.Agg("COUNT", None)
    if distinct and name != "COUNT":
        raise OutOfScope(f"{name}(DISTINCT ...); only COUNT(DISTINCT ...) is supported")
    return ir.Agg(name, _expr(arg), distinct)


def _text_arg(node: exp.Expression):
    # sqlglot wraps the argument of LEN / LEFT / RIGHT in CAST(... AS TEXT); undo that.
    # sqlglot 會把 LEN / LEFT / RIGHT 的參數包一層 CAST(... AS TEXT),這裡把它拆掉。
    if isinstance(node, exp.Cast) and node.args["to"].this.name in _TEXT_TYPES and not node.args["to"].expressions:
        return _expr(node.this)
    return _expr(node)


def _function(node: exp.Expression):
    kind = type(node)
    if kind in _SIMPLE_FUNCTIONS:
        _only(node, {"this"}, _SIMPLE_FUNCTIONS[kind])
        return ir.Func(_SIMPLE_FUNCTIONS[kind], (_expr(node.this),))
    if kind is exp.Trim:
        _only(node, {"this", "position"}, "TRIM")
        name = {"LEADING": "LTRIM", "TRAILING": "RTRIM", "": "TRIM"}.get((node.args.get("position") or "").upper())
        if name is None:
            raise OutOfScope("TRIM with an unusual position")
        return ir.Func(name, (_expr(node.this),))
    if kind is exp.Length:
        _only(node, {"this"}, "LEN")
        return ir.Func("LEN", (_text_arg(node.this),))
    if kind is exp.Substring:
        _only(node, {"this", "start", "length"}, "SUBSTRING")
        if node.args.get("start") is None or node.args.get("length") is None:
            raise OutOfScope("SUBSTRING needs a start and a length")
        return ir.Func("SUBSTRING", (_expr(node.this), _expr(node.args["start"]), _expr(node.args["length"])))
    if kind in (exp.Left, exp.Right):
        return ir.Func("LEFT" if kind is exp.Left else "RIGHT", (_text_arg(node.this), _expr(node.expression)))
    if kind is exp.Replace:
        _only(node, {"this", "expression", "replacement"}, "REPLACE")
        return ir.Func("REPLACE", (_expr(node.this), _expr(node.expression), _expr(node.args["replacement"])))
    if kind is exp.Concat:
        return ir.Func("CONCAT", tuple(_expr(e) for e in node.expressions))
    if kind is exp.Coalesce:
        args = (_expr(node.this),) + tuple(_expr(e) for e in node.expressions)
        return ir.Func("ISNULL" if node.args.get("is_null") else "COALESCE", args)
    if kind is exp.Nullif:
        return ir.Func("NULLIF", (_expr(node.this), _expr(node.expression)))
    if kind is exp.Round:
        _only(node, {"this", "decimals", "truncate"}, "ROUND")
        if node.args.get("decimals") is None:
            raise OutOfScope("ROUND needs a number of digits")
        truncate = node.args.get("truncate")
        truncating = truncate is not None and not (isinstance(truncate, exp.Literal) and truncate.this == "0")
        return ir.Func("TRUNCATE" if truncating else "ROUND", (_expr(node.this), _expr(node.args["decimals"])))

    if kind is exp.Window:
        raise OutOfScope("window function (OVER ...)")
    if kind in (exp.Subquery, exp.Exists, exp.Select):
        raise OutOfScope("sub-query used as a value or inside a condition")
    if kind is exp.Anonymous:
        raise OutOfScope(f"function {node.name.upper()} is not in the supported list")
    name = node.sql_name() if isinstance(node, exp.Func) else kind.__name__
    raise OutOfScope(f"{name} is not supported by IR v1")


assert set(_SIMPLE_FUNCTIONS.values()) <= set(FUNCTIONS), "parser emits a function the type rules do not know"
