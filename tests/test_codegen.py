"""Tests for engine.codegen and engine.ir_types: generated PySpark must behave like SQL Server.
engine.codegen 與 engine.ir_types 的測試:產生的 PySpark 行為必須與 SQL Server 相同。

Every expected value below was read off a real SQL Server (2016 SP1) while building
the rules; the tests run the generated code on Spark and require the same answers.
下列每個預期值都是建立規則時從真實的 SQL Server(2016 SP1)讀出來的;
測試會在 Spark 上執行產生的程式,並要求得到相同答案。

Test list / 測試清單
1. the same IR always generates exactly the same text
   同一份 IR 永遠產生完全相同的文字
2. integer / integer and % follow SQL Server: 7/2=3, -7/2=-3, -7%2=-1
   整數除法與取餘數依 SQL Server 行為
3. AVG over integers drops the fraction, toward zero: AVG(-7,-8) = -7
   整數的 AVG 捨去小數(朝零方向)
4. decimal division and AVG over decimals cut digits off instead of rounding
   小數除法與小數 AVG 是截斷而非四捨五入
5. the result type of decimal / whole-number constant matches SQL Server: decimal(22,6)
   小數除以整數常數的結果型別與 SQL Server 相同
6. SUM over no rows is NULL and COUNT is 0
   空集合的 SUM 為 NULL、COUNT 為 0
7. text comparison ignores case and trailing spaces; LEN ignores trailing spaces
   文字比較不分大小寫且忽略尾端空白;LEN 忽略尾端空白
8. GROUP BY on text merges values differing only by case / trailing spaces
   文字 GROUP BY 會合併只差大小寫/尾端空白的值
9. CASE sends NULL to ELSE and respects boundaries
   CASE 中 NULL 落入 ELSE,且邊界值正確
10. LEFT JOIN + ISNULL fills 0 and keeps the decimal type
    LEFT JOIN + ISNULL 補 0 並保留 decimal 型別
11. text + text is NULL if either side is NULL; CONCAT treats NULL as empty
    文字相加任一邊為 NULL 則為 NULL;CONCAT 把 NULL 當空字串
12. ROUND(x, 2, 1) truncates toward zero on decimals
    ROUND(x, 2, 1) 對小數做朝零截斷
13. NOT IN with a NULL in the list returns no rows
    清單含 NULL 的 NOT IN 不回傳任何列
14. invalid queries are refused: non-grouped column, unknown column, text mixed with numbers
    無效查詢會被拒絕:未分組欄位、未知欄位、文字與數字混用
"""
from decimal import Decimal as D

import pytest

from engine.codegen import RULE_INT_AVG, RULE_INT_DIVISION, generate
from engine.ir import OutOfScope
from engine.model import Column, Table
from engine.parser import parse_tsql
from engine.sandbox import run_code

T = {"t": [Column("id", "int"), Column("a", "int"), Column("b", "int"), Column("amt", "decimal(18,2)"), Column("s", "string")]}


def run(spark, sql, rows, catalog=T, extra=None):
    generated = generate(parse_tsql(sql), catalog)
    tables = {"t": Table(catalog["t"], rows, synthetic=True)}
    tables.update(extra or {})
    result = run_code(generated.code, spark, tables)
    return generated, result


def by_id(result):
    return {row[0]: row[1:] for row in result.rows}


def test_generation_is_deterministic(schemas):
    sql = "SELECT p.Region, AVG(p.VehAge) AS a, SUM(c.ClaimAmount) / COUNT(*) AS b FROM policy p LEFT JOIN claims c ON c.IDpol = p.IDpol GROUP BY p.Region"
    first, second = generate(parse_tsql(sql), schemas), generate(parse_tsql(sql), schemas)
    assert first.code == second.code and first.rules == second.rules


def test_integer_division_and_modulo(spark):
    generated, result = run(spark, "SELECT id, a / b AS q, a % b AS r FROM t", [(1, 7, 2, None, None), (2, -7, 2, None, None), (3, 7, None, None, None)])
    assert by_id(result) == {1: (3, 1), 2: (-3, -1), 3: (None, None)}
    assert RULE_INT_DIVISION in generated.rules
    assert [c.type for c in result.columns] == ["int", "int", "int"]


def test_avg_over_integers_truncates_toward_zero(spark):
    generated, result = run(spark, "SELECT AVG(a) AS av FROM t", [(1, -7, 0, None, None), (2, -8, 0, None, None), (3, None, 0, None, None)])
    assert result.rows == [(-7,)] and RULE_INT_AVG in generated.rules


def test_decimal_division_and_avg_truncate(spark):
    rows = [(1, 3, 0, D("2.00"), None), (2, 3, 0, D("-2.00"), None), (3, 6, 0, D("5.99"), None)]
    _, result = run(spark, "SELECT id, amt / 3 AS third, amt / a AS per FROM t", rows)
    assert by_id(result)[1] == (D("0.666666"), D("0.6666666666666"))
    assert by_id(result)[2] == (D("-0.666666"), D("-0.6666666666666"))
    _, avg = run(spark, "SELECT AVG(amt) AS av FROM t", [(1, 0, 0, D("1.00"), None), (2, 0, 0, D("1.00"), None), (3, 0, 0, D("0.00"), None)])
    assert avg.rows == [(D("0.666666"),)] and avg.columns[0].type == "decimal(38,6)"


def test_decimal_division_result_type_matches_sql_server(spark):
    _, result = run(spark, "SELECT id, amt / 3 AS by_constant, amt / a AS by_column FROM t", [(1, 3, 0, D("2.00"), None)])
    assert [c.type for c in result.columns[1:]] == ["decimal(22,6)", "decimal(29,13)"]


def test_aggregates_over_no_rows(spark):
    _, result = run(spark, "SELECT SUM(amt) AS total, COUNT(*) AS n, AVG(a) AS av FROM t", [])
    assert result.rows == [(None, 0, None)]
    _, grouped = run(spark, "SELECT s, SUM(amt) AS total FROM t GROUP BY s", [])
    assert grouped.rows == []


def test_text_comparison_ignores_case_and_trailing_spaces(spark):
    rows = [(1, 0, 0, None, "Diesel"), (2, 0, 0, None, "DIESEL  "), (3, 0, 0, None, " diesel"), (4, 0, 0, None, None)]
    _, result = run(spark, "SELECT id, LEN(s) AS n FROM t WHERE s = 'diesel'", rows)
    assert by_id(result) == {1: (6,), 2: (6,)}


def test_group_by_text_merges_case_variants(spark):
    rows = [(1, 0, 0, None, "abc"), (2, 0, 0, None, "ABC"), (3, 0, 0, None, "Abc "), (4, 0, 0, None, None)]
    _, result = run(spark, "SELECT s, COUNT(*) AS n FROM t GROUP BY s", rows)
    # Which spelling is shown for the merged group is not fixed, so compare ignoring case.
    # 合併後的群組顯示哪一種寫法並不固定,因此比對時不分大小寫。
    groups = {(s.strip().upper() if s is not None else None): n for s, n in result.rows}
    assert groups == {"ABC": 3, None: 1}


def test_case_null_goes_to_else_and_boundaries_hold(spark):
    sql = "SELECT id, CASE WHEN a < 50 THEN 'low' WHEN a = 50 THEN 'edge' WHEN a <= 100 THEN 'mid' ELSE 'other' END AS band FROM t"
    rows = [(1, 49, 0, None, None), (2, 50, 0, None, None), (3, 100, 0, None, None), (4, 101, 0, None, None), (5, None, 0, None, None)]
    assert by_id(run(spark, sql, rows)[1]) == {1: ("low",), 2: ("edge",), 3: ("mid",), 4: ("other",), 5: ("other",)}


def test_left_join_with_isnull_zero_fill(spark, schemas):
    sql = """SELECT p.IDpol, ISNULL(SUM(c.ClaimAmount), 0) AS total, COUNT(c.IDpol) AS n
             FROM policy p LEFT JOIN claims c ON c.IDpol = p.IDpol GROUP BY p.IDpol"""
    policy = Table(schemas["policy"], [(1, 0, 1.0, 1, 50, "D", "N"), (2, 0, 1.0, 1, 50, "D", "N"), (None, 0, 1.0, 1, 50, "D", "N")], True)
    claims = Table(schemas["claims"], [(1, D("10.50")), (1, D("0.25")), (None, D("99.00")), (7, D("1.00"))], True)
    result = run_code(generate(parse_tsql(sql), schemas).code, spark, {"policy": policy, "claims": claims})
    assert by_id(result) == {1: (D("10.75"), 2), 2: (D("0.00"), 0), None: (D("0.00"), 0)}
    assert result.columns[1].type == "decimal(38,2)"


def test_text_plus_and_concat_differ_on_null(spark):
    rows = [(1, 0, 0, None, "x"), (2, 0, 0, None, None)]
    _, result = run(spark, "SELECT id, s + '!' AS plus, CONCAT(s, '!') AS joined FROM t", rows)
    assert by_id(result) == {1: ("x!", "x!"), 2: (None, "!")}


def test_round_truncate_on_decimals(spark):
    catalog = {"t": [Column("id", "int"), Column("x", "decimal(10,6)")]}
    generated = generate(parse_tsql("SELECT id, ROUND(x, 2, 1) AS cut, ROUND(x, 2) AS rounded FROM t"), catalog)
    table = Table(catalog["t"], [(1, D("0.779000")), (2, D("-0.779000")), (3, None)], True)
    assert by_id(run_code(generated.code, spark, {"t": table})) == {
        1: (D("0.770000"), D("0.780000")), 2: (D("-0.770000"), D("-0.780000")), 3: (None, None),
    }


def test_not_in_with_null_returns_nothing(spark):
    rows = [(1, 5, 0, None, None), (2, 1, 0, None, None)]
    assert run(spark, "SELECT id FROM t WHERE a NOT IN (1, NULL)", rows)[1].rows == []
    assert run(spark, "SELECT id FROM t WHERE a NOT IN (1, 2)", rows)[1].rows == [(1,)]


@pytest.mark.parametrize("sql, word", [
    ("SELECT s, a FROM t GROUP BY s", "GROUP BY"),
    ("SELECT nope FROM t", "unknown column"),
    ("SELECT id FROM t WHERE s = 1", "incompatible"),
    ("SELECT a + s AS x FROM t", "non-numeric"),
    ("SELECT id FROM nowhere", "unknown table"),
    ("SELECT id FROM t WHERE a", "condition"),
    ("SELECT x.id FROM t x JOIN t y ON x.id = y.id WHERE id = 1", "ambiguous"),
])
def test_invalid_queries_are_refused(sql, word):
    with pytest.raises(OutOfScope) as caught:
        generate(parse_tsql(sql), T)
    assert word.lower() in caught.value.reason.lower()
