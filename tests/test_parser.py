"""Tests for engine.parser, engine.ir and engine.lineage (T-SQL -> IR).
engine.parser、engine.ir 與 engine.lineage 的測試(T-SQL -> IR)。

Test list / 測試清單
1. a join + group by + having query becomes the expected IR pieces
   join + group by + having 查詢會轉成預期的 IR 結構
2. IR survives a JSON round trip unchanged
   IR 轉成 JSON 再轉回來內容不變
3. ISNULL is kept distinct from COALESCE (their result types differ)
   ISNULL 與 COALESCE 不會被混為一談(兩者結果型別不同)
4. ROUND(x, n, 1) is recognised as truncation, ROUND(x, n) as rounding
   ROUND(x, n, 1) 被辨識為截斷,ROUND(x, n) 為四捨五入
5. BETWEEN, IN, NOT IN, IS NOT NULL and simple CASE are normalised
   BETWEEN、IN、NOT IN、IS NOT NULL、簡單 CASE 會被正規化
6. a CTE is inlined as a nested query
   CTE 會被展開成巢狀查詢
7. setup statements (SET, USE, DROP TABLE, GO) are ignored; INSERT ... SELECT is unwrapped
   環境設定陳述式會被忽略;INSERT ... SELECT 會被拆開
8. each unsupported construct is refused with a reason naming it
   每種不支援的語法都會被拒絕,且原因會指名該語法
9. text that is not SQL at all is refused, not crashed on
   完全不是 SQL 的文字會被拒絕,而不是當掉
10. lineage traces an output through a sub-query down to base columns
    lineage 能穿過子查詢追到最底層的來源欄位
"""
import pytest

from engine import ir
from engine.ir import OutOfScope
from engine.lineage import build_lineage, features
from engine.parser import parse_tsql


def test_join_group_having_structure():
    q = parse_tsql("""
        SELECT p.Region, COUNT(*) AS n, SUM(c.ClaimAmount) AS total
        FROM dbo.policy AS p LEFT JOIN claims c ON c.IDpol = p.IDpol
        WHERE p.Exposure > 0 GROUP BY p.Region HAVING COUNT(*) > 1 ORDER BY p.Region
    """)
    assert q.source == ir.Source(alias="p", table="policy")
    assert q.joins[0].kind == "left" and q.joins[0].source == ir.Source(alias="c", table="claims")
    assert q.joins[0].on == ir.Cmp("=", ir.Col("IDpol", "c"), ir.Col("IDpol", "p"))
    assert q.where == ir.Cmp(">", ir.Col("Exposure", "p"), ir.Lit(0, "int"))
    assert q.group_by == (ir.Col("Region", "p"),)
    assert q.having == ir.Cmp(">", ir.Agg("COUNT"), ir.Lit(1, "int"))
    assert [o.name for o in q.select] == ["Region", "n", "total"]
    assert q.select[2].expr == ir.Agg("SUM", ir.Col("ClaimAmount", "c"))


def test_ir_json_round_trip():
    q = parse_tsql("""
        SELECT x.a, CASE WHEN x.a IN (1, 2) THEN 'low' ELSE 'high' END AS band, 1.50 AS rate
        FROM (SELECT IDpol AS a FROM policy WHERE VehGas LIKE 'd%') x INNER JOIN claims c ON c.IDpol = x.a
    """)
    assert ir.from_json(ir.to_json(q)) == q


def test_isnull_is_not_coalesce():
    q = parse_tsql("SELECT ISNULL(a, 0) AS x, COALESCE(a, b, 0) AS y FROM policy")
    assert q.select[0].expr.name == "ISNULL" and q.select[1].expr.name == "COALESCE"
    assert len(q.select[1].expr.args) == 3


def test_round_truncate_flag():
    q = parse_tsql("SELECT ROUND(a, 2, 1) AS cut, ROUND(a, 2) AS rounded, ROUND(a, 2, 0) AS also_rounded FROM policy")
    assert [o.expr.name for o in q.select] == ["TRUNCATE", "ROUND", "ROUND"]


def test_condition_shapes_are_normalised():
    q = parse_tsql("""
        SELECT IDpol FROM policy
        WHERE VehAge BETWEEN 1 AND 5 AND BonusMalus NOT IN (50, 60) AND Region IS NOT NULL
          AND CASE VehGas WHEN 'Diesel' THEN 1 ELSE 0 END = 1
    """)
    # BETWEEN becomes two comparisons, which merge into the surrounding AND.
    # BETWEEN 會變成兩個比較,並併入外層的 AND。
    lower, upper, not_in, not_null, simple_case = q.where.args
    assert lower == ir.Cmp(">=", ir.Col("VehAge"), ir.Lit(1, "int"))
    assert upper == ir.Cmp("<=", ir.Col("VehAge"), ir.Lit(5, "int"))
    assert isinstance(not_in, ir.InList) and not_in.negated and len(not_in.items) == 2
    assert not_null == ir.IsNull(ir.Col("Region"), negated=True)
    assert simple_case.left.whens[0][0] == ir.Cmp("=", ir.Col("VehGas"), ir.Lit("Diesel", "string"))


def test_cte_is_inlined():
    q = parse_tsql("WITH big AS (SELECT IDpol FROM claims WHERE ClaimAmount > 100) SELECT b.IDpol FROM big AS b")
    assert q.source.alias == "b" and q.source.query.source.table == "claims"


def test_setup_statements_ignored_and_insert_unwrapped():
    q = parse_tsql("""
        SET NOCOUNT ON;
        USE insurance;
        GO
        DROP TABLE IF EXISTS dbo.out_j;
        INSERT INTO dbo.out_j SELECT IDpol FROM dbo.policy;
        GO
    """)
    assert q.source.table == "policy" and [o.name for o in q.select] == ["IDpol"]


@pytest.mark.parametrize("sql, word", [
    ("MERGE t USING s ON t.a = s.a WHEN MATCHED THEN UPDATE SET t.b = s.b;", "MERGE"),
    ("INSERT INTO d SELECT a FROM (MERGE t USING s ON t.a = s.a WHEN MATCHED THEN DELETE OUTPUT s.a) x", "MERGE"),
    ("SELECT IDpol, RANK() OVER (ORDER BY ClaimNb) AS r FROM policy", "window"),
    ("CREATE PROCEDURE p AS BEGIN SELECT 1 AS a END", "procedur"),
    ("SELECT a FROM t UNION ALL SELECT a FROM u", "UNION"),
    ("SELECT DISTINCT Region FROM policy", "DISTINCT"),
    ("SELECT TOP 5 IDpol FROM policy", "LIMIT"),
    ("SELECT * FROM policy", "SELECT *"),
    ("SELECT IDpol FROM policy p FULL JOIN claims c ON c.IDpol = p.IDpol", "FULL"),
    ("SELECT IDpol FROM policy p CROSS JOIN claims c", "CROSS"),
    ("SELECT IDpol FROM policy WHERE IDpol IN (SELECT IDpol FROM claims)", "IN"),
    ("SELECT GETDATE() AS now FROM policy", "not supported"),
    ("SELECT a INTO #t FROM policy; SELECT a FROM #t;", "statements"),
    ("SELECT IDpol + 1 FROM policy", "no name"),
])
def test_unsupported_constructs_are_refused_with_a_reason(sql, word):
    with pytest.raises(OutOfScope) as caught:
        parse_tsql(sql)
    assert word.lower() in caught.value.reason.lower()


@pytest.mark.parametrize("text", ["", "this is not sql at all ;;; (((", "data work.x; set y; run;", "SELECT FROM WHERE"])
def test_garbage_is_refused_not_crashed(text):
    with pytest.raises(OutOfScope):
        parse_tsql(text)


def test_lineage_traces_through_subquery(schemas):
    q = parse_tsql("""
        SELECT p.Region, SUM(t.total) AS RegionTotal
        FROM policy p LEFT JOIN (SELECT IDpol, SUM(ClaimAmount) AS total FROM claims GROUP BY IDpol) t ON t.IDpol = p.IDpol
        WHERE p.VehGas = 'Diesel' GROUP BY p.Region
    """)
    lineage = build_lineage(q, schemas)
    assert lineage["confidence"] == "derived" and lineage["tables"] == ["claims", "policy"]
    assert lineage["outputs"]["RegionTotal"]["sources"] == ["claims.ClaimAmount"]
    assert lineage["outputs"]["RegionTotal"]["operations"] == ["aggregate:SUM"]
    assert lineage["outputs"]["Region"]["sources"] == ["policy.Region"]
    assert lineage["row_filters"] == ["claims.IDpol", "policy.IDpol", "policy.VehGas"]
    assert {"join:left", "subquery", "group_by", "aggregate:SUM", "filter"} <= set(features(q))
