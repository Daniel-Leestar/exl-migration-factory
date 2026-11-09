"""Mock PySpark Solution for Job J00_mock.
J00_mock 作业的 Mock PySpark 迁移脚本实现。

Owner / 负责人: Migration Factory DevOps / 迁移工厂运维开发
Module Function / 模块功能:
  - Produces candidate PySpark DataFrame matching the authoritative oracle output.
    产出与权威基准标准答案完全匹配的候选 PySpark DataFrame。
"""

from typing import Any

# Conditional import for PySpark types
# 条件导入 PySpark 类型
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql.types import (
        StructType,
        StructField,
        LongType,
        IntegerType,
        DoubleType,
    )
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    SparkSession = Any  # type: ignore
    DataFrame = Any     # type: ignore


def run_job(spark: Any) -> Any:
    """Execute migration logic for J00_mock and return transformed DataFrame.
    执行 J00_mock 的迁移转换逻辑并返回目标 DataFrame。

    Args:
        spark: Active SparkSession or execution context / 活跃的 SparkSession 实例或运行上下文

    Returns:
        DataFrame containing migrated mock policy records / 包含迁移后策略模拟记录的 DataFrame
    """
    mock_records = [
        (101, 0, 250.50),
        (102, 1, 430.10),
        (103, 0, 180.00),
        (104, 2, 890.75),
        (105, 0, 310.20),
    ]

    if HAS_PYSPARK and spark is not None:
        # Schema definition matching meta.yaml / 匹配 meta.yaml 的 Schema 结构
        schema = StructType([
            StructField("IDpol", LongType(), False),
            StructField("ClaimNb", IntegerType(), False),
            StructField("EarnedPremium", DoubleType(), False),
        ])
        return spark.createDataFrame(mock_records, schema=schema)
    else:
        # Local non-JVM fallback using pandas / 本地无 JVM 环境的 pandas 降级实现
        import pandas as pd
        df = pd.DataFrame(mock_records, columns=["IDpol", "ClaimNb", "EarnedPremium"])
        df["IDpol"] = df["IDpol"].astype("int64")
        df["ClaimNb"] = df["ClaimNb"].astype("int32")
        df["EarnedPremium"] = df["EarnedPremium"].astype("float64")
        return df

