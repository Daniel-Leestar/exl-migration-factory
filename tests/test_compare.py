"""Tests for engine.recon.compare and engine.model (the reconciliation rules).
engine.recon.compare 與 engine.model 的測試(對帳規則)。

Test list / 測試清單
1. identical tables pass even when rows and columns are in a different order
   相同的表即使列序、欄序不同也通過
2. a changed cell is reported with its key, column and both values
   被改動的一格會連同主鍵、欄位與兩邊的值一起回報
3. missing and extra rows are counted separately
   缺少與多出的列分開計數
4. DECIMAL must match exactly: 1.50 vs 1.51 fails, 1.50 vs 1.5 passes
   DECIMAL 必須完全相等
5. FLOAT tolerates 1e-9 but not 1e-6
   FLOAT 容許 1e-9,不容許 1e-6
6. NULL equals only NULL (NULL vs 0 fails)
   NULL 只等於 NULL
7. without a primary key, duplicate rows must appear the same number of times
   沒有主鍵時,重複列的出現次數必須相同
8. repeated key values fall back to whole-row comparison and say so
   主鍵重複時退回整列比對並註明
9. case-insensitive columns ignore case and trailing spaces, also when used as the key
   不分大小寫的欄位會忽略大小寫與尾端空白,當主鍵時亦然
10. a missing output column fails the schema check
    少了輸出欄位時 schema 檢查失敗
11. a table survives a write/read round trip, keeping NULL distinct from empty text
    表經過寫出再讀回後不變,NULL 與空字串仍可區分
"""
from decimal import Decimal as D

from engine.model import Column, Table, read_table, tables_fingerprint, write_table
from engine.recon.compare import CompareRules, compare

COLS = [Column("id", "int"), Column("amount", "decimal(18,2)"), Column("name", "string")]


def table(rows, columns=COLS):
    return Table(columns, rows)


def test_same_data_in_different_order_passes():
    legacy = table([(1, D("1.50"), "a"), (2, D("2.00"), "b")])
    new = Table([COLS[2], COLS[0], COLS[1]], [("b", 2, D("2.00")), ("a", 1, D("1.50"))])
    report = compare(legacy, new, CompareRules(primary_key=["id"]))
    assert report["passed"] and report["mode"] == "primary_key"


def test_changed_cell_is_reported_with_key_and_values():
    report = compare(table([(1, D("1.50"), "a")]), table([(1, D("1.51"), "a")]), CompareRules(primary_key=["id"]))
    assert not report["passed"]
    assert report["checks"]["rows"]["different"] == 1
    assert report["samples"]["different"][0] == {"key": {"id": "1"}, "cells": [{"column": "amount", "legacy": "1.50", "new": "1.51"}]}


def test_missing_and_extra_rows_are_counted_separately():
    legacy = table([(1, D("1"), "a"), (2, D("2"), "b")])
    new = table([(2, D("2"), "b"), (3, D("3"), "c"), (4, D("4"), "d")])
    rows = compare(legacy, new, CompareRules(primary_key=["id"]))["checks"]["rows"]
    assert (rows["missing"], rows["extra"], rows["different"]) == (1, 2, 0)


def test_decimal_scale_is_ignored_but_value_is_exact():
    rules = CompareRules(primary_key=["id"])
    assert compare(table([(1, D("1.50"), "a")]), table([(1, D("1.5"), "a")]), rules)["passed"]
    assert not compare(table([(1, D("1.50"), "a")]), table([(1, D("1.5000001"), "a")]), rules)["passed"]


def test_float_tolerance():
    cols = [Column("id", "int"), Column("x", "float")]
    rules = CompareRules(primary_key=["id"])
    assert compare(table([(1, 0.1 + 0.2)], cols), table([(1, 0.3)], cols), rules)["passed"]
    assert not compare(table([(1, 0.3)], cols), table([(1, 0.300001)], cols), rules)["passed"]


def test_null_is_not_zero():
    report = compare(table([(1, None, "a")]), table([(1, D("0"), "a")]), CompareRules(primary_key=["id"]))
    assert not report["passed"]


def test_without_key_duplicates_must_match_in_number():
    twice, once = [(1, D("1"), "a")] * 2, [(1, D("1"), "a")]
    assert compare(table(twice), table(list(twice)), CompareRules())["passed"]
    report = compare(table(twice), table(once), CompareRules())
    assert not report["passed"] and report["mode"] == "multiset"
    assert report["checks"]["rows"]["missing"] == 1


def test_repeated_keys_fall_back_to_whole_rows():
    rows = [(1, D("1"), "a"), (1, D("2"), "b")]
    report = compare(table(rows), table(list(reversed(rows))), CompareRules(primary_key=["id"]))
    assert report["passed"] and report["mode"] == "multiset" and "repeat" in report["note"]


def test_case_insensitive_columns_also_as_key():
    cols = [Column("region", "string"), Column("n", "int")]
    rules = CompareRules(primary_key=["region"], ci_columns=["region"])
    assert compare(table([("North ", 1)], cols), table([("NORTH", 1)], cols), rules)["passed"]
    assert not compare(table([("North", 1)], cols), table([("NORTH", 1)], cols), CompareRules(primary_key=["region"]))["passed"]


def test_missing_column_fails_schema_check():
    report = compare(table([(1, D("1"), "a")]), Table(COLS[:2], [(1, D("1"))]), CompareRules(primary_key=["id"]))
    assert not report["passed"] and report["checks"]["schema"]["missing_in_new"] == ["name"]


def test_table_file_round_trip_keeps_null_and_empty_apart(tmp_path):
    original = table([(1, D("1.50"), ""), (2, None, None), (3, D("-0.01"), "a,b \"q\"")])
    write_table(original, tmp_path / "t.csv")
    loaded = read_table(tmp_path / "t.csv")
    assert loaded.columns == original.columns and loaded.rows == original.rows
    assert tables_fingerprint({"t": loaded}) == tables_fingerprint({"t": original})
    assert tables_fingerprint({"t": loaded}) != tables_fingerprint({"t": table([(1, D("1.50"), None)])})
