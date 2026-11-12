"""Tests for engine.pipeline: the whole factory, with stand-ins for SQL Server and the LLM.
engine.pipeline 的測試:整條工廠流程,以替身取代 SQL Server 與 LLM。

The stand-in "legacy system" is a hand-written PySpark reference, so each test
controls exactly what the legacy answer is.
「舊系統」替身是手寫的 PySpark 參考實作,因此每個測試都能完全掌控舊系統的答案。

Test list / 測試清單
1. a job inside IR scope that matches legacy is Verified, and all evidence files are written
   IR 範圍內且與舊系統一致的 job 為 Verified,並寫出所有證據檔
2. when rule-generated code disagrees with legacy the job is Needs review, with a counterexample of at most 3 rows
   規則產生的程式與舊系統不一致時為 Needs review,並附上最多 3 列的反例
3. an out-of-scope job fixed by the LLM on the first repair is Tested, with every call logged
   超出範圍的 job 經 LLM 第一次修正後通過為 Tested,且每次呼叫都有紀錄
4. an LLM that never gets it right is stopped after exactly 3 repairs -> Needs review
   始終翻不對的 LLM 在剛好 3 次修正後被停止 -> Needs review
5. LLM code that tries to read a file is rejected and never executed
   試圖讀檔的 LLM 程式會被拒絕且不會執行
6. garbage input, unknown tables and a missing job never raise; they become Needs review
   垃圾輸入、未知的表、不存在的 job 都不會丟例外,而是變成 Needs review
7. with no legacy answer at all the job is Needs review, never Verified
   完全沒有舊系統答案時為 Needs review,絕不會是 Verified
8. answers recorded once can be replayed without the legacy system (the CI mode)
   錄下的答案可以在沒有舊系統的情況下重播(CI 模式)
9. run_all writes a status table whose summary counts add up
   run_all 寫出的狀態表,其摘要數字加總正確
10. (needs SQL Server) the sample job J04 is Verified against a real SQL Server
    (需要 SQL Server)樣本 job J04 對真實 SQL Server 驗證為 Verified
"""
import json
import os
from pathlib import Path

import pytest

from engine.job import load_schemas
from engine.llm.client import FakeLLM
from engine.model import Column
from engine.pipeline import NEEDS_REVIEW, TESTED, VERIFIED, Context, run_all, run_job
from engine.sandbox import run_code

SCHEMAS = {"t": [Column("id", "int"), Column("a", "int"), Column("b", "int")]}
ROOT = Path(__file__).resolve().parent.parent

DIVIDE_SQL = "SELECT id, a / b AS q FROM t"
RANK_SQL = "SELECT id, a, RANK() OVER (ORDER BY a) AS r FROM t"

HEAD = "from pyspark.sql import functions as F, Window\n\ndef run(spark, tables):\n"
TRUNCATING_DIVIDE = HEAD + "    return tables['t'].select('id', (F.col('a') / F.col('b')).cast('int').alias('q'))\n"
FLOORING_DIVIDE = HEAD + "    return tables['t'].select('id', F.floor(F.col('a') / F.col('b')).cast('int').alias('q'))\n"
RANK = HEAD + "    return tables['t'].select('id', 'a', F.rank().over(Window.orderBy('a')).alias('r'))\n"
DENSE_RANK = HEAD + "    return tables['t'].select('id', 'a', F.dense_rank().over(Window.orderBy('a')).alias('r'))\n"
READS_FILE = HEAD + "    return spark.read.csv('C:/secret.csv')\n"


class FakeLegacy:
    """Plays the legacy system by running a reference implementation.
    以執行參考實作的方式扮演舊系統。
    """

    can_rerun = True

    def __init__(self, spark, reference_code):
        self.spark, self.code, self.runs = spark, reference_code, 0

    def run(self, job, tables, dataset):
        self.runs += 1
        return run_code(self.code, self.spark, tables)


def answer(code):
    return f"Here you go:\n```python\n{code}```"


def make_job(tmp_path, sql, name="JX"):
    folder = tmp_path / "jobs" / name
    folder.mkdir(parents=True)
    (folder / "legacy.sql").write_text(sql, encoding="utf-8")
    (folder / "meta.yaml").write_text(f"id: {name}\ninputs: [t]\noutput:\n  primary_key: [id]\n", encoding="utf-8")
    return folder


def make_ctx(tmp_path, spark, reference=None, llm=None, **options):
    ctx = Context(schemas=SCHEMAS, out_dir=tmp_path / "out", llm=llm, adversarial_rows=24, shrink_trials=40, **options)
    ctx._spark = spark
    if reference is not None:
        ctx.live = FakeLegacy(spark, reference)
    return ctx


def test_matching_ir_job_is_verified_with_evidence(tmp_path, spark):
    entry = run_job(make_job(tmp_path, DIVIDE_SQL), make_ctx(tmp_path, spark, TRUNCATING_DIVIDE))
    assert (entry["level"], entry["path"], entry["repairs"], entry["model"]) == (VERIFIED, "ir", 0, None)
    assert {d["name"]: d["passed"] for d in entry["datasets"]} == {"adversarial": True, "empty": True}
    assert any("integer / integer" in rule for rule in entry["rules_applied"])
    out = tmp_path / "out" / "JX"
    for name in ("ir.json", "lineage.json", "generated.py", "recon_report.json", "status.json", "legacy.sql"):
        assert (out / name).exists(), name
    assert not (out / "ai_log.jsonl").exists()
    lineage = json.loads((out / "lineage.json").read_text(encoding="utf-8"))
    assert lineage["outputs"]["q"]["sources"] == ["t.a", "t.b"] and lineage["confidence"] == "derived"
    assert entry["human_review"]["status"] == "pending"


def test_rule_mismatch_is_needs_review_with_small_counterexample(tmp_path, spark):
    # Legacy here rounds down (-7 / 2 = -4) while the rules truncate (-3): a real disagreement.
    # 這裡的舊系統是無條件捨去(-7 / 2 = -4),而規則是截斷(-3):確實不一致。
    entry = run_job(make_job(tmp_path, DIVIDE_SQL), make_ctx(tmp_path, spark, FLOORING_DIVIDE))
    assert entry["level"] == NEEDS_REVIEW and entry["path"] == "ir" and "differs from legacy" in entry["reason"]
    report = json.loads((tmp_path / "out" / "JX" / "recon_report.json").read_text(encoding="utf-8"))
    example = report["counterexample"]
    assert example["shrunk_to_rows"] <= 3 < example["shrunk_from_rows"]
    (row,) = example["inputs"]["t"]["rows"]
    assert row[1] * row[2] < 0 and row[1] % row[2] != 0  # a negative, inexact quotient / 負的、除不盡的商


def test_llm_job_fixed_on_first_repair_is_tested(tmp_path, spark):
    llm = FakeLLM([answer(DENSE_RANK), answer(RANK)], model="fake-1")
    entry = run_job(make_job(tmp_path, RANK_SQL), make_ctx(tmp_path, spark, RANK, llm))
    assert (entry["level"], entry["path"], entry["repairs"], entry["model"]) == (TESTED, "llm", 1, "fake-1")
    assert "window" in entry["out_of_scope_reason"]
    out = tmp_path / "out" / "JX"
    log = [json.loads(line) for line in (out / "ai_log.jsonl").read_text(encoding="utf-8").splitlines()]
    assert [record["purpose"] for record in log] == ["translate", "repair 1"]
    assert "Expected output (legacy system)" in log[1]["prompt"] and "dense_rank" in log[1]["prompt"]
    code = (out / "generated.py").read_text(encoding="utf-8")
    assert code.startswith('"""AI-GENERATED by fake-1') and "F.rank()" in code
    assert json.loads((out / "lineage.json").read_text(encoding="utf-8"))["confidence"] == "inferred, unverified"
    assert not (out / "ir.json").exists()


def test_llm_that_never_succeeds_stops_after_three_repairs(tmp_path, spark):
    llm = FakeLLM(lambda system, prompt: answer(DENSE_RANK))
    entry = run_job(make_job(tmp_path, RANK_SQL), make_ctx(tmp_path, spark, RANK, llm))
    assert entry["level"] == NEEDS_REVIEW and entry["repairs"] == 3 and len(llm.calls) == 4
    assert "after 3 repair" in entry["reason"]


def test_llm_code_that_reads_files_is_rejected(tmp_path, spark):
    llm = FakeLLM(lambda system, prompt: answer(READS_FILE))
    entry = run_job(make_job(tmp_path, RANK_SQL), make_ctx(tmp_path, spark, RANK, llm))
    assert entry["level"] == NEEDS_REVIEW and "not allowed" in entry["reason"]
    assert "not allowed" in llm.calls[1][1]  # the rejection is fed back to the model / 拒絕原因會回饋給模型


def test_bad_inputs_never_raise(tmp_path, spark):
    ctx = make_ctx(tmp_path, spark, TRUNCATING_DIVIDE)
    garbage = tmp_path / "jobs" / "garbage.sql"
    garbage.parent.mkdir(parents=True)
    garbage.write_text("%%% this is ((( not SQL", encoding="utf-8")
    entry = run_job(garbage, ctx)
    assert entry["level"] == NEEDS_REVIEW and "LLM path is switched off" in entry["reason"]

    folder = make_job(tmp_path, DIVIDE_SQL, "JBAD")
    (folder / "meta.yaml").write_text("id: JBAD\ninputs: [no_such_table]\n", encoding="utf-8")
    assert "not in the schema file" in run_job(folder, ctx)["reason"]

    missing = run_job(tmp_path / "jobs" / "does_not_exist", ctx)
    assert missing["level"] == NEEDS_REVIEW and "could not be loaded" in missing["reason"]


def test_no_legacy_answer_means_needs_review(tmp_path, spark):
    entry = run_job(make_job(tmp_path, DIVIDE_SQL), make_ctx(tmp_path, spark))
    assert entry["level"] == NEEDS_REVIEW and "no legacy output" in entry["reason"]
    assert (tmp_path / "out" / "JX" / "generated.py").exists()


def test_recorded_answers_replay_without_legacy(tmp_path, spark):
    folder = make_job(tmp_path, DIVIDE_SQL)
    recording = make_ctx(tmp_path, spark, TRUNCATING_DIVIDE, record_golden=True)
    assert run_job(folder, recording)["level"] == VERIFIED
    assert (folder / "golden" / "adversarial.csv").exists()

    replay = run_job(folder, make_ctx(tmp_path, spark))
    assert replay["level"] == VERIFIED and all(d["legacy_status"] == "ok" for d in replay["datasets"])

    # An answer file made from other inputs must not be trusted.
    # 由不同輸入產生的答案檔不可採信。
    (folder / "golden" / "adversarial.sha256").write_text("0" * 64, encoding="utf-8")
    stale = run_job(folder, make_ctx(tmp_path, spark))
    assert stale["level"] == NEEDS_REVIEW and "adversarial" in stale["reason"]


def test_run_all_status_table(tmp_path, spark):
    make_job(tmp_path, DIVIDE_SQL, "J1")
    make_job(tmp_path, RANK_SQL, "J2")
    (tmp_path / "jobs" / "_schemas.yaml").write_text("t: {id: int}\n", encoding="utf-8")
    status = run_all(tmp_path / "jobs", make_ctx(tmp_path, spark, TRUNCATING_DIVIDE))
    assert [job["id"] for job in status["jobs"]] == ["J1", "J2"]
    assert status["summary"] == {VERIFIED: 1, TESTED: 0, NEEDS_REVIEW: 1}
    assert json.loads((tmp_path / "out" / "status.json").read_text(encoding="utf-8"))["summary"] == status["summary"]


@pytest.mark.skipif(not os.environ.get("MF_SQLSERVER_CONN"), reason="MF_SQLSERVER_CONN is not set")
def test_sample_job_against_real_sql_server(tmp_path, spark):
    from engine.legacy.runner import SqlServerRunner

    ctx = Context(schemas=load_schemas(ROOT / "jobs" / "_schemas.yaml"), out_dir=tmp_path / "out")
    ctx._spark = spark
    ctx.live = SqlServerRunner(os.environ["MF_SQLSERVER_CONN"], database="mf_test")
    entry = run_job(ROOT / "jobs" / "J04", ctx)
    assert entry["level"] == VERIFIED, entry["reason"]
    assert any("integer / integer" in rule for rule in entry["rules_applied"])
