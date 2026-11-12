"""Automated Reconciliation Gate Test Suite.
自动化双轨对账门禁测试套件。

Owner / 负责人: Migration Factory DevOps / 迁移工厂运维开发
Module Function / 模块功能:
  - Dynamically scans `jobs/` directory for validated job packages containing `oracle_output.parquet` and `solution.py`.
    动态扫描 `jobs/` 目录下包含 `oracle_output.parquet` 与 `solution.py` 的合规作业包。
  - Executes 3-tier mathematical parity checks between Spark output and authoritative Oracle ground truth:
    在 PySpark 输出与权威基准标准答案之间执行三重数学一致性对账检验:
      1. Row Count Check / 全量基数检验 (Count parity)
      2. Sum / Checksum Check / 数值指纹总和检验 (Bounded precision)
      3. Record-for-record Parity / 逐行全外连接比对 (0 missing, 0 extra, 0 mismatches)
  - Enforces CI/CD pipeline blocking gate (returns exit code 1 upon any discrepancy).
    作为 CI/CD 强门禁（一旦发现任何数据偏差立即抛出异常阻断合入）。
"""

import os
import glob
import math
import importlib.util
from typing import Dict, List, Any, Tuple, Optional, Generator
import pytest
import yaml
import pandas as pd

# Conditional import for PySpark
# 条件导入 PySpark
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    SparkSession = Any  # type: ignore
    DataFrame = Any     # type: ignore

# Check whether JVM runtime is active for PySpark
# 检测本地是否存在可用的 Java JVM 运行时
IS_SPARK_AVAILABLE = False
if HAS_PYSPARK:
    try:
        _test_spark = SparkSession.builder.master("local[1]").appName("ProbeReconciler").getOrCreate()
        _test_spark.stop()
        IS_SPARK_AVAILABLE = True
    except Exception:
        IS_SPARK_AVAILABLE = False


@pytest.fixture(scope="session")
def spark_session() -> Generator[Any, None, None]:
    """PySpark session fixture for reconciliation gate.
    用于自动化对账门禁的 PySpark 会话 Fixture。
    """
    if not IS_SPARK_AVAILABLE:
        yield None
    else:
        spark = (
            SparkSession.builder
            .master("local[1]")
            .appName("TestReconciliationGate")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        yield spark
        spark.stop()


# ==============================================================================
# Job Package Scanner
# 作业包动态发现器
# ==============================================================================

class JobPackage:
    """Represents a self-contained migration job package.
    表示一个独立、自包含的迁移作业包。
    """
    def __init__(self, job_id: str, job_dir: str, meta: Dict[str, Any], oracle_path: str, solution_path: str):
        self.job_id = job_id
        self.job_dir = job_dir
        self.meta = meta
        self.oracle_path = oracle_path
        self.solution_path = solution_path

    def load_solution_module(self) -> Any:
        """Dynamically load solution.py module.
        动态加载作业的 solution.py 模块。
        """
        spec = importlib.util.spec_from_file_location(f"solution_{self.job_id}", self.solution_path)
        if spec is None or spec.loader is None:
            raise ImportError(f"Cannot load solution module from {self.solution_path}")
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        return module


def discover_reconciliation_jobs(jobs_root: str = "jobs") -> List[JobPackage]:
    """Scan jobs directory and discover all eligible job packages.
    扫描 jobs 目录并发现所有满足对账条件的作业包。
    """
    packages = []
    if not os.path.exists(jobs_root):
        return packages

    for entry in os.listdir(jobs_root):
        job_dir = os.path.join(jobs_root, entry)
        if not os.path.isdir(job_dir):
            continue

        meta_file = os.path.join(job_dir, "meta.yaml")
        oracle_file = os.path.join(job_dir, "oracle_output.parquet")
        solution_file = os.path.join(job_dir, "solution.py")

        if os.path.exists(meta_file) and os.path.exists(oracle_file) and os.path.exists(solution_file):
            with open(meta_file, "r", encoding="utf-8") as f:
                meta = yaml.safe_load(f) or {}
            packages.append(JobPackage(
                job_id=meta.get("job_id", entry),
                job_dir=job_dir,
                meta=meta,
                oracle_path=oracle_file,
                solution_path=solution_file,
            ))

    return packages


# ==============================================================================
# Core 3-Tier Reconciliation Logic
# 核心三重对账比对逻辑
# ==============================================================================

def reconcile_spark_against_oracle(
    spark_df: Any,
    oracle_df: pd.DataFrame,
    meta: Dict[str, Any],
) -> Dict[str, Any]:
    """Execute row-by-row reconciliation between candidate Spark output and Oracle ground truth.
    在候选 Spark 输出与 Oracle 权威结果集之间执行全量逐行对账。

    Args:
        spark_df: Spark DataFrame or Pandas DataFrame (in local fallback) / Spark 或 Pandas DataFrame
        oracle_df: Baseline ground-truth DataFrame loaded from oracle_output.parquet / Oracle 基准数据
        meta: Job metadata dictionary from meta.yaml / 来自 meta.yaml 的元数据

    Returns:
        Reconciliation audit report dictionary / 对账审计报告字典
    """
    # Normalize candidate Spark output to pandas for deterministic attribute inspection
    # 将候选结果转换为 Pandas 结构以便进行高精度、确定性的逐字段对账与反例提取
    if hasattr(spark_df, "toPandas"):
        df_candidate = spark_df.toPandas()
    else:
        df_candidate = pd.DataFrame(spark_df).copy()

    df_oracle = oracle_df.copy()
    job_id = meta.get("job_id", "UNKNOWN")
    primary_keys = meta.get("primary_keys", [])
    columns_spec = {col["name"]: col for col in meta.get("columns", [])}

    report = {
        "job_id": job_id,
        "passed": True,
        "row_count_check": {"passed": False, "spark_count": len(df_candidate), "oracle_count": len(df_oracle)},
        "checksum_check": {"passed": True, "details": []},
        "record_parity_check": {"passed": True, "missing_in_spark": 0, "extra_in_spark": 0, "mismatched_rows": 0, "mismatches": []},
        "failure_reasons": [],
    }

    # --------------------------------------------------------------------------
    # Check 1: Row Count Parity / 基数一致性检验
    # --------------------------------------------------------------------------
    spark_count = len(df_candidate)
    oracle_count = len(df_oracle)
    if spark_count == oracle_count:
        report["row_count_check"]["passed"] = True
    else:
        report["passed"] = False
        report["row_count_check"]["passed"] = False
        diff_count = spark_count - oracle_count
        report["failure_reasons"].append(
            f"Row count mismatch / 行数不一致: Spark={spark_count}, Oracle={oracle_count} (Diff={diff_count})"
        )

    # --------------------------------------------------------------------------
    # Check 2: Checksum & Totals Parity / 数值指纹与和值比对
    # --------------------------------------------------------------------------
    for col_name, spec in columns_spec.items():
        col_type = spec.get("type", "").upper()
        if col_type in ["FLOAT", "DOUBLE", "INT", "BIGINT", "DECIMAL"]:
            if col_name in df_candidate.columns and col_name in df_oracle.columns:
                spark_sum = float(df_candidate[col_name].sum())
                oracle_sum = float(df_oracle[col_name].sum())
                abs_tol = float(spec.get("abs_tolerance", 1.0e-6))
                delta = abs(spark_sum - oracle_sum)
                passed = delta <= abs_tol
                report["checksum_check"]["details"].append({
                    "column": col_name,
                    "spark_sum": spark_sum,
                    "oracle_sum": oracle_sum,
                    "abs_diff": delta,
                    "tolerance": abs_tol,
                    "passed": passed,
                })
                if not passed:
                    report["passed"] = False
                    report["checksum_check"]["passed"] = False
                    report["failure_reasons"].append(
                        f"Checksum mismatch on {col_name} / 字段 {col_name} 和值校验失败: Delta={delta} > Tolerance={abs_tol}"
                    )

    # --------------------------------------------------------------------------
    # Check 3: Full Outer Join Record-for-Record Parity / 主键全外连接逐行比对
    # --------------------------------------------------------------------------
    if not primary_keys:
        report["passed"] = False
        report["failure_reasons"].append("Missing primary_keys in meta.yaml / meta.yaml 缺少主键声明")
        return report

    merged = pd.merge(
        df_oracle,
        df_candidate,
        on=primary_keys,
        how="outer",
        suffixes=("_oracle", "_spark"),
        indicator=True,
    )

    missing_in_spark = int((merged["_merge"] == "left_only").sum())
    extra_in_spark = int((merged["_merge"] == "right_only").sum())
    report["record_parity_check"]["missing_in_spark"] = missing_in_spark
    report["record_parity_check"]["extra_in_spark"] = extra_in_spark

    if missing_in_spark > 0:
        report["passed"] = False
        report["record_parity_check"]["passed"] = False
        report["failure_reasons"].append(f"Missing rows in Spark output / Spark 丢失记录数: {missing_in_spark}")

    if extra_in_spark > 0:
        report["passed"] = False
        report["record_parity_check"]["passed"] = False
        report["failure_reasons"].append(f"Phantom extra rows in Spark output / Spark 多余脏记录数: {extra_in_spark}")

    # Inspect matching rows for column-level value discrepancy
    # 比对主键吻合行在各项非主键字段上的数值一致性
    both = merged[merged["_merge"] == "both"]
    mismatched_indices = set()

    for col_name, spec in columns_spec.items():
        if col_name in primary_keys:
            continue
        c_oracle = f"{col_name}_oracle"
        c_spark = f"{col_name}_spark"
        if c_oracle in both.columns and c_spark in both.columns:
            compare_mode = spec.get("compare_mode", "exact")
            abs_tol = float(spec.get("abs_tolerance", 1.0e-9))

            for idx, row in both.iterrows():
                v_oracle = row[c_oracle]
                v_spark = row[c_spark]

                is_match = False
                if pd.isna(v_oracle) and pd.isna(v_spark):
                    is_match = True
                elif compare_mode == "tolerance":
                    try:
                        is_match = abs(float(v_oracle) - float(v_spark)) <= abs_tol
                    except (ValueError, TypeError):
                        is_match = False
                else:
                    is_match = (v_oracle == v_spark)

                if not is_match:
                    mismatched_indices.add(idx)
                    pk_val = {pk: row[pk] for pk in primary_keys}
                    report["record_parity_check"]["mismatches"].append({
                        "pk": pk_val,
                        "column": col_name,
                        "oracle_val": v_oracle,
                        "spark_val": v_spark,
                    })

    mismatched_count = len(mismatched_indices)
    report["record_parity_check"]["mismatched_rows"] = mismatched_count
    if mismatched_count > 0:
        report["passed"] = False
        report["record_parity_check"]["passed"] = False
        report["failure_reasons"].append(
            f"Value discrepancies detected in {mismatched_count} rows / 检测到 {mismatched_count} 行存在字段值不一致"
        )

    return report


# ==============================================================================
# Pytest Test Cases for CI/CD Gate
# 用于 CI/CD 门禁的 Pytest 测试用例
# ==============================================================================

def test_dynamic_reconciliation_gate(spark_session: Any):
    """Dynamic reconciliation gate: validates all discovered job packages against oracle parquet.
    动态对账门禁：针对所有发现的作业包执行严格双轨对账验证。
    """
    discovered_jobs = discover_reconciliation_jobs("jobs")
    assert len(discovered_jobs) > 0, "No valid job packages discovered in jobs/ directory / 未在 jobs/ 目录下发现有效作业包"

    print("\n" + "=" * 65)
    print("  CI/CD AUTOMATED RECONCILIATION GATE EXECUTION")
    print("=" * 65)

    passed_jobs = []
    failed_jobs = []

    for job in discovered_jobs:
        print(f"\n>>> Reconciling Job Package: [{job.job_id}] ({job.meta.get('name', '')})")
        # Load authoritative oracle parquet / 加载基准 parquet
        oracle_df = pd.read_parquet(job.oracle_path)

        # Run candidate Spark solution / 运行候选 PySpark 脚本
        mod = job.load_solution_module()
        assert hasattr(mod, "run_job"), f"Job {job.job_id} solution.py must define 'run_job(spark)'"
        candidate_output = mod.run_job(spark_session)

        # Execute reconciliation check / 执行对账校验
        report = reconcile_spark_against_oracle(candidate_output, oracle_df, job.meta)

        if report["passed"]:
            passed_jobs.append(job.job_id)
            print(f"  [PARITY SUCCESS] Job {job.job_id} passed all 3 checks (0 discrepancies)!")
        else:
            failed_jobs.append((job.job_id, report["failure_reasons"]))
            print(f"  [PARITY FAILURE] Job {job.job_id} failed reconciliation:")
            for reason in report["failure_reasons"]:
                print(f"    - {reason}")

    # Output executive gate summary
    # 输出对账门禁执行汇总大屏
    print("\n" + "=" * 65)
    print("  RECONCILIATION GATE SUMMARY / 自动化对账门禁汇总")
    print("=" * 65)
    print(f"  Total Checked Jobs / 校验作业总数   : {len(discovered_jobs)}")
    print(f"  Passed Jobs / 通过作业数           : {len(passed_jobs)}")
    print(f"  Failed Jobs / 阻断作业数           : {len(failed_jobs)}")
    status_label = "ALL PASSED (100% PARITY)" if len(failed_jobs) == 0 else "GATE BLOCKED"
    print(f"  Gate Status / 门禁最终状态         : {status_label}")
    print("=" * 65 + "\n")

    # Assert 0 failures to enforce blocking behavior
    # 断言失败数为 0 以确保 CI 阻断行为
    assert len(failed_jobs) == 0, f"Reconciliation Gate failed for jobs: {failed_jobs}"


def test_reconciliation_negative_gate_blocks_mismatches():
    """Negative test case: ensure that simulated discrepancies are strictly caught and blocked.
    反向测试用例：确保人工模拟的数据偏差会被门禁严密拦截并阻断。
    """
    mock_meta = {
        "job_id": "J_TEST_CORRUPT",
        "primary_keys": ["IDpol"],
        "columns": [
            {"name": "IDpol", "type": "BIGINT", "compare_mode": "exact"},
            {"name": "EarnedPremium", "type": "FLOAT", "compare_mode": "tolerance", "abs_tolerance": 1.0e-9},
        ],
    }

    oracle_df = pd.DataFrame([
        {"IDpol": 1, "EarnedPremium": 100.0},
        {"IDpol": 2, "EarnedPremium": 200.0},
    ])

    # Corrupted candidate output (value mismatch and missing row)
    # 模拟缺陷代码产出（金额篡改与掉单）
    corrupted_spark_df = pd.DataFrame([
        {"IDpol": 1, "EarnedPremium": 999.0},  # Discrepancy! / 偏差
    ])

    report = reconcile_spark_against_oracle(corrupted_spark_df, oracle_df, mock_meta)

    # Gate must report failure and detect exact mismatch reasons
    # 门禁必须判为失败并精准捕捉原因
    assert not report["passed"], "Corrupted candidate output must NOT pass reconciliation gate"
    assert not report["row_count_check"]["passed"]
    assert report["record_parity_check"]["missing_in_spark"] == 1
    assert report["record_parity_check"]["mismatched_rows"] >= 1

