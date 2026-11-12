"""Bronze Layer Ingestion Script.
Bronze 原始层数据摄取脚本。

Owner / 负责人: Tianyue Li / 李天悦
Module Function / 模块功能:
  - Ingest raw CSV files (freMTPL2freq.csv, freMTPL2sev.csv) into Delta Lake Bronze layer.
    将原始 CSV (freMTPL2freq.csv, freMTPL2sev.csv) 摄取至 Delta Lake Bronze 层。
  - Supports automatic fallback to lightweight pandas mode if PySpark/Java runtime is unavailable locally.
    若本地未配置 PySpark/Java 运行环境，支持自动平滑降级至本地 Pandas 轻量模式，确保开发非阻断。
Contract Specification / 契约规范:
  - Storage path / 存储路径: data/bronze/<table_name>
  - Format / 格式: Delta Lake (or CSV/Parquet fallback for local non-Java sandbox)
  - Audit columns / 稽核字段: `_ingested_at`, `_source_file`
"""

import os
import sys
from typing import Any
from datetime import datetime, timezone

# Conditional import for PySpark to allow CLI and helper usage across environments
# 条件导入 PySpark，允许跨环境安全调用辅助工具
try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import current_timestamp, input_file_name
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    SparkSession = Any  # type: ignore


def create_spark_session() -> SparkSession:
    """Create and configure SparkSession with Delta Lake package support.
    创建并配置支持 Delta Lake 的 SparkSession。
    """
    if not HAS_PYSPARK:
        raise ImportError(
            "PySpark is not installed in the current environment. Please install requirements.txt.\n"
            "当前环境未安装 PySpark。请先安装 requirements.txt。"
        )
    return (
        SparkSession.builder
        .appName("BronzeIngestion")
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()
    )


def ingest_raw_to_bronze_pyspark(spark: SparkSession, raw_path: str, bronze_path: str, table_name: str) -> str:
    """Ingest raw CSV to Delta Lake using PySpark engine.
    使用 PySpark 引擎将原始 CSV 摄取为 Delta Lake 表。
    """
    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw CSV file not found / 未找到原始 CSV 文件: {raw_path}")

    # Read Raw CSV preserving original schema types
    # 读取 Raw CSV 并保留原始类型
    df_raw = spark.read.option("header", "true").option("inferSchema", "true").csv(raw_path)

    # Append data engineering audit columns
    # 附加数据工程稽核字段
    df_bronze = (
        df_raw
        .withColumn("_ingested_at", current_timestamp())
        .withColumn("_source_file", input_file_name())
    )

    output_dir = os.path.join(bronze_path, table_name)
    df_bronze.write.format("delta").mode("overwrite").save(output_dir)
    print(f"[Bronze Ingest: PySpark] Successfully ingested {table_name} into {output_dir}")
    return output_dir


def ingest_raw_to_bronze_fallback(raw_path: str, bronze_path: str, table_name: str) -> str:
    """Lightweight local ingestion fallback using pandas when PySpark/Java is absent.
    当缺少 PySpark/Java 环境时，使用 Pandas 进行本地轻量摄取降级。
    """
    import pandas as pd

    if not os.path.exists(raw_path):
        raise FileNotFoundError(f"Raw CSV file not found / 未找到原始 CSV 文件: {raw_path}")

    output_dir = os.path.join(bronze_path, table_name)
    os.makedirs(output_dir, exist_ok=True)
    out_file = os.path.join(output_dir, f"{table_name}_bronze.csv")

    print(f"[Bronze Ingest: Local Fallback] Processing {raw_path} with pandas...")
    now_utc = datetime.now(timezone.utc).isoformat()
    df = pd.read_csv(raw_path)
    df["_ingested_at"] = now_utc
    df["_source_file"] = os.path.basename(raw_path)
    df.to_csv(out_file, index=False)

    print(f"[Bronze Ingest: Local Fallback] Successfully created Bronze mock table: {out_file} (Rows: {len(df)})")
    return output_dir


def ingest_raw_to_bronze(spark: Any, raw_path: str, bronze_path: str, table_name: str) -> str:
    """Unified ingestion entrypoint with automatic local fallback.
    统一摄取入口，具备自动本地降级能力。
    """
    if HAS_PYSPARK and spark is not None:
        try:
            return ingest_raw_to_bronze_pyspark(spark, raw_path, bronze_path, table_name)
        except Exception as e:
            print(f"[Warning] PySpark execution failed ({e}), falling back to local mode...")
            return ingest_raw_to_bronze_fallback(raw_path, bronze_path, table_name)
    else:
        return ingest_raw_to_bronze_fallback(raw_path, bronze_path, table_name)


# Aliases for backward compatibility with test suites
# 提供向后兼容测试套件的函数别名
get_spark_session = create_spark_session
ingest_csv_to_bronze = lambda spark, csv_path, table_name, bronze_root="data/bronze": ingest_raw_to_bronze(
    spark, csv_path, bronze_root, table_name
)


if __name__ == "__main__":
    raw_dir = "data/raw"
    bronze_dir = "data/bronze"

    # Try initializing PySpark; if not available, gracefully use local fallback
    # 尝试初始化 PySpark；若环境不满足，则优雅切换至本地轻量模式
    spark = None
    if HAS_PYSPARK:
        try:
            spark = create_spark_session()
        except Exception as e:
            print(f"[Notice / 提示] PySpark JVM not available ({e}). Using local fallback mode / 切换至本地轻量降级模式.")
    else:
        print("[Notice / 提示] PySpark not installed. Running in lightweight local fallback mode / 未检测到 PySpark，以本地轻量模式运行.")

    # Ingest French Motor Insurance datasets
    # 摄取法国车险数据集
    ingest_raw_to_bronze(spark, f"{raw_dir}/freMTPL2freq.csv", bronze_dir, "freMTPL2freq")
    ingest_raw_to_bronze(spark, f"{raw_dir}/freMTPL2sev.csv", bronze_dir, "freMTPL2sev")
