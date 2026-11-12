"""Tests for the smaller building blocks: adversarial data, shrinking, sandbox check, LLM helpers, job loading.
較小元件的測試:刁鑽資料、縮減、沙箱檢查、LLM 輔助函數、job 載入。

Test list / 測試清單
1. adversarial data is identical for the same seed and marked synthetic
   相同種子產生相同的刁鑽資料,且標記為合成
2. it contains the nasty cases: NULL, repeated keys, mixed case, trailing spaces, negatives
   內含刁鑽案例:NULL、重複鍵、大小寫混雜、尾端空白、負數
3. constants the job compares against appear together with both neighbours
   job 比較用的常數會連同兩側鄰值一起出現
4. a column used as a divisor never receives 0
   被當成除數的欄位不會出現 0
5. shrinking finds the two rows that jointly cause a failure, across two tables
   縮減能找出跨兩張表、共同造成失敗的兩列
6. shrinking stops at the re-run limit and still returns a failing input
   縮減在重跑上限時停止,且回傳的輸入仍然會失敗
7. the sandbox refuses file access, forbidden imports, JVM access and a missing run()
   沙箱拒絕檔案存取、禁用的 import、JVM 存取,以及缺少 run()
8. the sandbox accepts ordinary PySpark code, including Window
   沙箱接受一般的 PySpark 程式(含 Window)
9. code is extracted from a chatty model answer; an answer without run() is an error
   能從夾雜文字的模型回答取出程式;沒有 run() 的回答視為錯誤
10. a failure description includes row values for synthetic data only
    失敗描述只有在合成資料時才包含資料列的值
11. every model call is logged with prompt, answer and model name
    每次模型呼叫都記錄 prompt、回答與模型名稱
12. a bare .sql file loads as a job with defaults; a folder without code is an error
    單一 .sql 檔可用預設值載入成 job;沒有程式碼的資料夾會報錯
"""
import json
from decimal import Decimal as D

import pytest

from engine.job import JobError, load_job
from engine.llm.client import FakeLLM, LLMError, LoggedLLM
from engine.llm.translator import describe_failure, extract_code
from engine.model import Column, Table
from engine.parser import parse_tsql
from engine.recon.advdata import empty_tables, generate_adversarial
from engine.recon.compare import CompareRules, compare
from engine.recon.shrink import shrink
from engine.sandbox import SandboxError, check_code


def column(tables, table, name):
    index = [c.name for c in tables[table].columns].index(name)
    return [row[index] for row in tables[table].rows]


def test_adversarial_data_is_repeatable_and_synthetic(schemas):
    first, second = generate_adversarial(schemas, seed=3), generate_adversarial(schemas, seed=3)
    assert all(first[t].rows == second[t].rows and first[t].synthetic for t in schemas)
    assert generate_adversarial(schemas, seed=4)["policy"].rows != first["policy"].rows
    assert all(not t.rows and t.synthetic for t in empty_tables(schemas).values())


def test_adversarial_data_contains_nasty_cases(schemas):
    tables = generate_adversarial(schemas)
    ids, regions, ages = column(tables, "policy", "IDpol"), column(tables, "policy", "Region"), column(tables, "policy", "VehAge")
    assert None in ids and len(ids) > len(set(ids))
    assert {"A", "a", "A "} <= set(regions) and None in regions
    assert any(v is not None and v < 0 for v in ages)
    assert set(ids) & set(column(tables, "claims", "IDpol")) - {None}


def test_adversarial_data_hits_boundaries_from_the_job(schemas):
    query = parse_tsql("SELECT IDpol, CASE WHEN BonusMalus <= 137 THEN 'a' ELSE 'b' END AS t FROM policy WHERE VehGas = 'Diesel'")
    tables = generate_adversarial(schemas, query)
    assert {136, 137, 138} <= set(column(tables, "policy", "BonusMalus"))
    assert {"Diesel", "DIESEL", "diesel", "Diesel "} <= set(column(tables, "policy", "VehGas"))


def test_divisor_columns_never_get_zero(schemas):
    query = parse_tsql("SELECT IDpol, ClaimNb / VehAge AS r FROM policy")
    assert 0 not in column(generate_adversarial(schemas, query), "policy", "VehAge")
    assert 0 not in column(generate_adversarial(schemas, None, "select ClaimNb / p.VehAge from policy p"), "policy", "VehAge")
    assert 0 in column(generate_adversarial(schemas), "policy", "VehAge")


def test_shrink_finds_the_two_guilty_rows():
    cols = [Column("id", "int")]
    inputs = {"a": Table(cols, [(i,) for i in range(20)], True), "b": Table(cols, [(i,) for i in range(100, 120)], True)}

    def still_fails(candidate):
        return (7,) in candidate["a"].rows and (113,) in candidate["b"].rows

    small, reruns = shrink(inputs, still_fails)
    assert small["a"].rows == [(7,)] and small["b"].rows == [(113,)]
    assert 0 < reruns <= 120 and small["a"].synthetic


def test_shrink_respects_the_rerun_limit():
    inputs = {"a": Table([Column("id", "int")], [(i,) for i in range(64)], True)}
    calls = []

    def still_fails(candidate):
        calls.append(1)
        return (5,) in candidate["a"].rows

    small, reruns = shrink(inputs, still_fails, max_trials=3)
    assert reruns == len(calls) == 3 and (5,) in small["a"].rows


@pytest.mark.parametrize("body, word", [
    ("def run(spark, tables):\n    return open('x')", "open"),
    ("import os\ndef run(spark, tables):\n    return tables['t']", "os"),
    ("import subprocess\ndef run(spark, tables):\n    return tables['t']", "subprocess"),
    ("def run(spark, tables):\n    return spark._jvm", "_jvm"),
    ("def run(spark, tables):\n    return spark.read.csv('x')", "read"),
    ("def run(spark, tables):\n    return tables['t'].__class__", "__class__"),
    ("def go(spark, tables):\n    return tables['t']", "run(spark, tables)"),
    ("def run(spark, tables)\n    return 1", "not valid Python"),
])
def test_sandbox_refuses_unsafe_code(body, word):
    with pytest.raises(SandboxError) as caught:
        check_code(body)
    assert word in str(caught.value)


def test_sandbox_accepts_ordinary_pyspark():
    check_code(
        "from decimal import Decimal\nfrom pyspark.sql import functions as F, Window\n\n"
        "def run(spark, tables):\n"
        "    w = Window.partitionBy('a').orderBy(F.desc('b'))\n"
        "    return tables['t'].withColumn('r', F.rank().over(w)).where(F.col('b') > F.lit(Decimal('1.5')))\n"
    )


def test_extract_code_from_model_answer():
    answer = "Sure! Here is the code:\n```python\nfrom pyspark.sql import functions as F\n\ndef run(spark, tables):\n    return tables['t']\n```\nHope it helps."
    assert extract_code(answer).startswith("from pyspark.sql") and extract_code(answer).endswith("return tables['t']\n")
    with pytest.raises(LLMError):
        extract_code("I cannot translate this.")


def test_failure_description_withholds_real_values():
    cols = [Column("id", "int"), Column("amount", "decimal(18,2)")]

    def describe(synthetic):
        inputs = {"t": Table(cols, [(1, D("987.65"))], synthetic)}
        legacy, new = Table(cols, [(1, D("987.65"))], synthetic), Table(cols, [(1, D("123.45"))], synthetic)
        return describe_failure(inputs, legacy, new, compare(legacy, new, CompareRules(primary_key=["id"])))

    assert "987.65" in describe(True) and "123.45" in describe(True)
    real = describe(False)
    assert "987.65" not in real and "123.45" not in real and "withheld" in real and "amount" in real


def test_model_calls_are_logged(tmp_path):
    llm = LoggedLLM(FakeLLM(["first answer"], model="test-model"), tmp_path / "ai_log.jsonl", "J99")
    llm.purpose = "translate"
    assert llm.complete("system text", "prompt text") == "first answer"
    with pytest.raises(LLMError):
        llm.complete("system text", "second prompt")
    records = [json.loads(line) for line in (tmp_path / "ai_log.jsonl").read_text(encoding="utf-8").splitlines()]
    assert records[0]["response"] == "first answer" and records[0]["model"] == "test-model"
    assert records[0]["job"] == "J99" and records[0]["purpose"] == "translate" and records[0]["prompt"] == "prompt text"
    assert "error" in records[1] and "response" not in records[1]


def test_job_loading(tmp_path):
    bare = tmp_path / "mystery.sql"
    bare.write_text("SELECT 1 AS a FROM t", encoding="utf-8")
    job = load_job(bare)
    assert (job.id, job.language, job.inputs, job.primary_key) == ("mystery", "tsql", [], [])
    folder = tmp_path / "J50"
    folder.mkdir()
    with pytest.raises(JobError):
        load_job(folder)
    (folder / "legacy.sas").write_text("data a; set b; run;", encoding="utf-8")
    (folder / "meta.yaml").write_text("id: J50\ninputs: [claims]\noutput:\n  primary_key: [IDpol]\n", encoding="utf-8")
    job = load_job(folder)
    assert (job.id, job.language, job.inputs, job.primary_key) == ("J50", "sas", ["claims"], ["IDpol"])
