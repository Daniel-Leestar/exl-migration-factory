"""Column-level lineage derived from the IR: where every output column comes from.
由 IR 推導的欄位層級 lineage:每個輸出欄位來自哪裡。

Because it is computed from the same IR the code is generated from, this lineage
is exact ("derived"), unlike lineage guessed by an LLM ("inferred").
因為它與產生程式用的是同一份 IR,所以這份 lineage 是精確的(derived),
不同於由 LLM 猜測的 lineage(inferred)。
"""
from __future__ import annotations

from engine import ir
from engine.ir_types import resolve_query
from engine.model import Column


def build_lineage(query: ir.Query, catalog: dict[str, list[Column]]) -> dict:
    """Return JSON-ready lineage for a query.
    回傳可直接存成 JSON 的 lineage。
    """
    query = resolve_query(query, catalog)
    conditions = [j.on for j in query.joins] + [c for c in (query.where, query.having) if c is not None]
    return {
        "method": "ir",
        "confidence": "derived",
        "tables": sorted(_tables(query)),
        "outputs": {
            output.name: {
                "sources": sorted(trace(output.expr, query)),
                "expression": ir.show(output.expr),
                "operations": sorted(_operations(output.expr)),
            }
            for output in query.select
        },
        # Columns that decide which rows appear, without being copied into any output.
        # 決定哪些列會出現、但不會被複製到輸出的欄位。
        "row_filters": sorted(set().union(*[trace(c, query) for c in conditions]) if conditions else []),
        "group_by": sorted(set().union(*[trace(g, query) for g in query.group_by]) if query.group_by else []),
    }


def features(query: ir.Query) -> list[str]:
    """Short labels for the constructs a query uses (for analytics and test data).
    查詢用到哪些語法的簡短標籤(供分析與測試資料使用)。
    """
    found = set()

    def visit(q: ir.Query):
        for source in [q.source] + [j.source for j in q.joins]:
            if source.query is not None:
                found.add("subquery")
                visit(source.query)
        found.update(f"join:{j.kind}" for j in q.joins)
        if q.where is not None:
            found.add("filter")
        if q.group_by:
            found.add("group_by")
        if q.having is not None:
            found.add("having")
        for item in _expressions(q):
            for node in ir.walk(item):
                if isinstance(node, ir.Agg):
                    found.add(f"aggregate:{node.func}")
                elif isinstance(node, ir.Case):
                    found.add("case")
                elif isinstance(node, ir.Func):
                    found.add(f"function:{node.name}")
                elif isinstance(node, ir.BinOp):
                    found.add("division" if node.op == "/" else "arithmetic")
                elif isinstance(node, ir.Like):
                    found.add("like")
                elif isinstance(node, ir.Cast):
                    found.add("cast")

    visit(query)
    return sorted(found)


def _expressions(q: ir.Query) -> list:
    items = [o.expr for o in q.select] + [j.on for j in q.joins] + list(q.group_by)
    return items + [c for c in (q.where, q.having) if c is not None]


def _sources(q: ir.Query) -> dict[str, ir.Source]:
    return {s.alias.lower(): s for s in [q.source] + [j.source for j in q.joins]}


def _tables(q: ir.Query) -> set[str]:
    tables = set()
    for source in _sources(q).values():
        tables |= _tables(source.query) if source.query is not None else {source.table}
    return tables


def trace(expr, q: ir.Query) -> set[str]:
    """Base `table.column` names an expression depends on, following sub-queries down.
    運算式所依賴的基礎 `表.欄位`,會一路往下追進子查詢。
    """
    sources = _sources(q)
    found = set()
    for node in ir.walk(expr):
        if not isinstance(node, ir.Col):
            continue
        source = sources[node.table.lower()]
        if source.query is None:
            found.add(f"{source.table}.{node.name}")
            continue
        for output in source.query.select:
            if output.name.lower() == node.name.lower():
                found |= trace(output.expr, source.query)
    return found


def _operations(expr) -> set[str]:
    labels = set()
    for node in ir.walk(expr):
        if isinstance(node, ir.Agg):
            labels.add(f"aggregate:{node.func}")
        elif isinstance(node, ir.Case):
            labels.add("case")
        elif isinstance(node, ir.BinOp):
            labels.add("arithmetic")
        elif isinstance(node, ir.Func):
            labels.add(f"function:{node.name}")
        elif isinstance(node, ir.Cast):
            labels.add("cast")
    return labels


def inferred_lineage(legacy_code: str, schemas: dict[str, list[Column]]) -> dict:
    """Best-effort lineage for jobs without IR: which known columns the code mentions by name.
    給沒有 IR 的 job 的盡力而為 lineage:程式碼中以名稱提到了哪些已知欄位。

    This is a text match, not an analysis, and is labelled as unverified.
    這只是文字比對而非分析,並標示為未驗證。
    """
    import re

    words = {w.lower() for w in re.findall(r"[A-Za-z_][A-Za-z0-9_]*", legacy_code)}
    mentioned = {
        table: [c.name for c in columns if c.name.lower() in words]
        for table, columns in schemas.items()
        if table.lower() in words
    }
    return {
        "method": "text-match",
        "confidence": "inferred, unverified",
        "tables": sorted(mentioned),
        "candidate_source_columns": sorted(f"{t}.{c}" for t, cols in mentioned.items() for c in cols),
        "outputs": {},
    }
