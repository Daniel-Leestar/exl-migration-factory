"""Bronze Layer Ingestion Script.
Bronze 原始层数据摄取脚本。

Owner / 负责人: Tianyue Li / 李天悦
Module Function / 模块功能:
  - Ingest raw CSV files (freMTPL2freq.csv, freMTPL2sev.csv) into Delta Lake Bronze layer.
    将原始 CSV (freMTPL2freq.csv, freMTPL2sev.csv) 摄取至 Delta Lake Bronze 层。
Contract Specification / 契约规范:
  - Storage path / 存储路径: data/bronze/<table_name>
  - Format / 格式: Delta Lake
  - Audit columns / 稽核字段: `_ingested_at`, `_source_file`
"""

import os
from typing import Any

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


def ingest_raw_to_bronze(spark: SparkSession, raw_path: str, bronze_path: str, table_name: str) -> str:
    """Read Raw CSV, preserve original schema, append audit metadata, and write to Bronze Delta table.
    读取 Raw CSV 并保留原始形态，仅附加数据工程稽核字段，并写入 Bronze 层 Delta 表。

    Args:
        spark: Active SparkSession / 活跃的 SparkSession 实例
        raw_path: Source raw CSV path / 源端原始 CSV 文件路径
        bronze_path: Bronze layer root directory / Bronze 层根存储目录
        table_name: Target Delta table name / 目标 Delta 表名

    Returns:
        Output directory path / 输出目标目录路径
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

    # Write to Delta Lake in Bronze layer
    # 写入 Bronze 层 Delta Lake 表
    df_bronze.write.format("delta").mode("overwrite").save(output_dir)
    print(f"Successfully ingested {table_name} into {output_dir}")
    return output_dir


# Aliases for backward compatibility with test suites
# 提供向后兼容测试套件的函数别名
get_spark_session = create_spark_session
ingest_csv_to_bronze = lambda spark, csv_path, table_name, bronze_root="data/bronze": ingest_raw_to_bronze(
    spark, csv_path, bronze_root, table_name
)


if __name__ == "__main__":
    # Standard Bronze pipeline execution
    # 标准 Bronze 管道执行入口
    spark = create_spark_session()
    raw_dir = "data/raw"
    bronze_dir = "data/bronze"

    # Ingest French Motor Insurance datasets
    # 摄取法国车险数据集
    ingest_raw_to_bronze(spark, f"{raw_dir}/freMTPL2freq.csv", bronze_dir, "freMTPL2freq")
    ingest_raw_to_bronze(spark, f"{raw_dir}/freMTPL2sev.csv", bronze_dir, "freMTPL2sev")
