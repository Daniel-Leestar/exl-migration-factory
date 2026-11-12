"""Silver Layer Data Contracts and Quarantine Unit Tests.
Silver 层数据契约与隔离区单元测试套件。

Owner / 负责人: Tianyue Li / 李天悦
Test Objectives / 测试目标:
  - Verify that valid records flow correctly into the Silver layer with proper type casting and string standardization.
    验证合规记录正确流入 Silver 层，并完成类型转换与字符串标准化。
  - Verify that boundary violation cases (Exposure=0, Exposure=-0.5, DrivAge=17, ClaimAmount<=0, NULL IDpol)
    are accurately routed to the Quarantine layer with correct failure tags.
    验证边界违规案例 (Exposure=0, Exposure=-0.5, DrivAge=17, ClaimAmount<=0, NULL IDpol) 精准路由至 Quarantine 隔离区并附带对应标签。
"""

import os
import pytest
from typing import Generator, Any
from cloud.silver_contracts import (
    REASON_IDPOL_NULL,
    REASON_EXPOSURE_NON_POSITIVE,
    REASON_EXPOSURE_EXCEEDS_MAX,
    REASON_DRIVAGE_UNDERAGE,
    REASON_CLAIMNB_NEGATIVE,
    REASON_BONUSMALUS_TOO_LOW,
    REASON_CLAIMAMOUNT_NON_POSITIVE,
    validate_and_cleanse_freq_spark,
    validate_and_cleanse_sev_spark,
    validate_and_cleanse_freq_pandas,
    validate_and_cleanse_sev_pandas,
    HAS_PYSPARK,
)

# Check whether JVM runtime is actually callable for PySpark
# 检测本地是否存在可用的 Java JVM 运行时
IS_SPARK_AVAILABLE = False
if HAS_PYSPARK:
    try:
        from pyspark.sql import SparkSession
        _test_spark = SparkSession.builder.master("local[1]").appName("Probe").getOrCreate()
        _test_spark.stop()
        IS_SPARK_AVAILABLE = True
    except Exception:
        IS_SPARK_AVAILABLE = False


@pytest.fixture(scope="session")
def spark_session() -> Generator[Any, None, None]:
    """PySpark session fixture for testing.
    用于测试的 PySpark Session Fixture。
    """
    if not IS_SPARK_AVAILABLE:
        pytest.skip("PySpark or Java runtime is not available in current environment / 当前环境缺少 PySpark 或 Java 运行时")

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("TestSilverContracts")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )
    yield spark
    spark.stop()


# ==============================================================================
# PySpark Contract Tests (Run when PySpark + JVM are present)
# PySpark 契约测试集 (当环境具备 PySpark 与 JVM 时执行)
# ==============================================================================

@pytest.mark.skipif(not IS_SPARK_AVAILABLE, reason="Requires active PySpark and Java runtime")
def test_spark_freq_valid_and_boundary_cases(spark_session: Any):
    """Test valid rows and boundary violations for freMTPL2freq using PySpark.
    使用 PySpark 测试 freMTPL2freq 的合规记录与边界违规分流。
    """
    test_data = [
        # Row 1: Valid row / 合规记录
        (101, 0, 0.50, 5, 2, 35, 50, "b12", "regular", "c", "rhone-alpes"),
        # Row 2: NULL IDpol / 空主键
        (None, 0, 0.50, 5, 2, 35, 50, "B12", "Regular", "C", "Rhone-Alpes"),
        # Row 3: Exposure = 0 / 承保期为 0
        (103, 0, 0.0, 5, 2, 35, 50, "B12", "Regular", "C", "Rhone-Alpes"),
        # Row 4: Exposure = -0.5 / 承保期为负
        (104, 0, -0.50, 5, 2, 35, 50, "B12", "Regular", "C", "Rhone-Alpes"),
        # Row 5: Exposure = 1.2 (exceeds max) / 承保期超上限
        (105, 0, 1.20, 5, 2, 35, 50, "B12", "Regular", "C", "Rhone-Alpes"),
        # Row 6: DrivAge = 17 (underage) / 驾驶年龄未满 18 岁
        (106, 0, 0.80, 5, 2, 17, 50, "B12", "Regular", "C", "Rhone-Alpes"),
        # Row 7: Blank Area / Region -> should default to UNKNOWN / 空白区域默认填充
        (107, 1, 0.40, 4, 1, 28, 60, "b01", "diesel", "", None),
    ]

    columns = [
        "IDpol", "ClaimNb", "Exposure", "VehPower", "VehAge",
        "DrivAge", "BonusMalus", "VehBrand", "VehGas", "Area", "Region"
    ]

    df_raw = spark_session.createDataFrame(test_data, columns)
    df_silver, df_quarantine, metrics = validate_and_cleanse_freq_spark(df_raw)

    # 1. Assert Silver count and standardization
    # 验证 Silver 层合格数量与大写填充规则
    silver_rows = df_silver.collect()
    assert len(silver_rows) == 2, f"Expected 2 silver records, got {len(silver_rows)}"

    # Check that Row 1 strings are uppercased
    row_101 = [r for r in silver_rows if r["IDpol"] == 101][0]
    assert row_101["VehBrand"] == "B12"
    assert row_101["VehGas"] == "REGULAR"
    assert row_101["Area"] == "C"

    # Check that Row 7 defaults are set to UNKNOWN
    row_107 = [r for r in silver_rows if r["IDpol"] == 107][0]
    assert row_107["Area"] == "UNKNOWN"
    assert row_107["Region"] == "UNKNOWN"

    # 2. Assert Quarantine routing and violation tags
    # 验证 Quarantine 隔离区路由与具体违规标签
    quar_rows = df_quarantine.collect()
    assert len(quar_rows) == 5, f"Expected 5 quarantined records, got {len(quar_rows)}"

    # Check individual failure tags
    quar_dict = {r["IDpol"]: r["_quarantine_reasons"] for r in quar_rows}
    assert REASON_IDPOL_NULL in quar_dict[None]
    assert REASON_EXPOSURE_NON_POSITIVE in quar_dict[103]
    assert REASON_EXPOSURE_NON_POSITIVE in quar_dict[104]
    assert REASON_EXPOSURE_EXCEEDS_MAX in quar_dict[105]
    assert REASON_DRIVAGE_UNDERAGE in quar_dict[106]


@pytest.mark.skipif(not IS_SPARK_AVAILABLE, reason="Requires active PySpark and Java runtime")
def test_spark_sev_valid_and_boundary_cases(spark_session: Any):
    """Test valid rows and boundary violations for freMTPL2sev using PySpark.
    使用 PySpark 测试 freMTPL2sev 的合规记录与边界违规分流。
    """
    test_data = [
        (201, 1500.50),  # Valid row / 合规记录
        (None, 800.00),  # NULL IDpol / 空主键
        (203, 0.00),     # ClaimAmount = 0 / 赔款为 0
        (204, -120.00),  # ClaimAmount < 0 / 赔款为负数
    ]
    columns = ["IDpol", "ClaimAmount"]

    df_raw = spark_session.createDataFrame(test_data, columns)
    df_silver, df_quarantine, metrics = validate_and_cleanse_sev_spark(df_raw)

    silver_rows = df_silver.collect()
    assert len(silver_rows) == 1
    assert silver_rows[0]["IDpol"] == 201

    quar_rows = df_quarantine.collect()
    assert len(quar_rows) == 3

    quar_dict = {r["IDpol"]: r["_quarantine_reasons"] for r in quar_rows}
    assert REASON_IDPOL_NULL in quar_dict[None]
    assert REASON_CLAIMAMOUNT_NON_POSITIVE in quar_dict[203]
    assert REASON_CLAIMAMOUNT_NON_POSITIVE in quar_dict[204]


# ==============================================================================
# Dual-Environment Contract Tests (Always runs via Pandas fallback)
# 双环境通用契约测试 (通过 Pandas 本地降级逻辑运行，确保本地无 Java 亦可严格校验)
# ==============================================================================

def test_fallback_freq_contracts_boundary_cases():
    """Verify freMTPL2freq contract boundary rules on local test dataset.
    在本地测试数据集上验证 freMTPL2freq 契约边界规则。
    """
    test_data = {
        "IDpol": [101, None, 103, 104, 105, 106, 107],
        "ClaimNb": [0, 0, 0, 0, 0, 0, 1],
        "Exposure": [0.5, 0.5, 0.0, -0.5, 1.2, 0.8, 0.4],
        "VehPower": [5, 5, 5, 5, 5, 5, 4],
        "VehAge": [2, 2, 2, 2, 2, 2, 1],
        "DrivAge": [35, 35, 35, 35, 35, 17, 28],
        "BonusMalus": [50, 50, 50, 50, 50, 50, 60],
        "VehBrand": ["b12", "B12", "B12", "B12", "B12", "B12", "b01"],
        "VehGas": ["regular", "Regular", "Regular", "Regular", "Regular", "Regular", "diesel"],
        "Area": ["c", "C", "C", "C", "C", "C", ""],
        "Region": ["rhone-alpes", "Rhone-Alpes", "Rhone-Alpes", "Rhone-Alpes", "Rhone-Alpes", "Rhone-Alpes", None],
    }

    df_silver, df_quarantine, metrics = validate_and_cleanse_freq_pandas(test_data)

    # Silver assertions
    assert len(df_silver) == 2
    row_101 = df_silver[df_silver["IDpol"] == 101].iloc[0]
    assert row_101["VehBrand"] == "B12"
    assert row_101["Area"] == "C"

    row_107 = df_silver[df_silver["IDpol"] == 107].iloc[0]
    assert row_107["Area"] == "UNKNOWN"
    assert row_107["Region"] == "UNKNOWN"

    # Quarantine assertions
    assert len(df_quarantine) == 5

    def get_reasons(id_val):
        sub = df_quarantine[df_quarantine["IDpol"].isna()] if id_val is None else df_quarantine[df_quarantine["IDpol"] == id_val]
        return sub.iloc[0]["_quarantine_reasons"]

    assert REASON_IDPOL_NULL in get_reasons(None)
    assert REASON_EXPOSURE_NON_POSITIVE in get_reasons(103)
    assert REASON_EXPOSURE_NON_POSITIVE in get_reasons(104)
    assert REASON_EXPOSURE_EXCEEDS_MAX in get_reasons(105)
    assert REASON_DRIVAGE_UNDERAGE in get_reasons(106)


def test_fallback_sev_contracts_boundary_cases():
    """Verify freMTPL2sev contract boundary rules on local test dataset.
    在本地测试数据集上验证 freMTPL2sev 契约边界规则。
    """
    test_data = {
        "IDpol": [201, None, 203, 204],
        "ClaimAmount": [1500.50, 800.0, 0.0, -120.0],
    }

    df_silver, df_quarantine, metrics = validate_and_cleanse_sev_pandas(test_data)

    assert len(df_silver) == 1
    assert df_silver.iloc[0]["IDpol"] == 201

    assert len(df_quarantine) == 3

    def get_reasons(id_val):
        sub = df_quarantine[df_quarantine["IDpol"].isna()] if id_val is None else df_quarantine[df_quarantine["IDpol"] == id_val]
        return sub.iloc[0]["_quarantine_reasons"]

    assert REASON_IDPOL_NULL in get_reasons(None)
    assert REASON_CLAIMAMOUNT_NON_POSITIVE in get_reasons(203)
    assert REASON_CLAIMAMOUNT_NON_POSITIVE in get_reasons(204)
