"""Bronze Layer Ingestion Script

负责人: 李天悦 (Tianyue)
功能: 将原始 CSV (如 freMTPL2freq.csv, freMTPL2sev.csv) 摄取至 Delta Lake Bronze 层
契约规范:
  - 存储路径: data/bronze/<table_name>
  - 格式: Delta Lake
  - 契约约束: 保留原始 CSV 结构，严格追加审计元数据 `_ingested_at`, `_source_file`，采用只追加 (Append) 模式
"""

import os
import sys
import argparse
from pathlib import Path
from pyspark.sql import SparkSession
from pyspark.sql import functions as F


def get_spark_session(app_name: str = "BronzeIngestion") -> SparkSession:
    """创建并配置支持 Delta Lake 的 PySpark Session"""
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )
    return builder.getOrCreate()


def ingest_csv_to_bronze(
    spark: SparkSession,
    csv_path: str,
    table_name: str,
    bronze_root: str = "data/bronze",
) -> str:
    """将单个 CSV 文件摄取至 Delta Lake Bronze 原始层

    Args:
        spark: SparkSession 实例
        csv_path: 原始 CSV 文件路径
        table_name: Bronze 目标表名 (例如 freMTPL2freq)
        bronze_root: Bronze 存储根路径

    Returns:
        写入的目标路径
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"Source CSV file not found: {csv_path}")

    target_dir = os.path.join(bronze_root, table_name)
    file_name = os.path.basename(csv_path)

    print(f"[Bronze Ingest] Reading CSV: {csv_path}")
    df_raw = (
        spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(csv_path)
    )

    # 按照全员契约规范，追加审计字段，保持原始字段不变
    df_bronze = (
        df_raw.withColumn("_ingested_at", F.current_timestamp())
        .withColumn("_source_file", F.lit(file_name))
    )

    print(f"[Bronze Ingest] Writing to Delta Lake (Append Mode): {target_dir}")
    (
        df_bronze.write.format("delta")
        .mode("append")
        .save(target_dir)
    )

    print(f"[Bronze Ingest] Completed successfully: {table_name} -> {target_dir}")
    return target_dir


def batch_ingest_raw(
    spark: SparkSession,
    raw_dir: str = "data/raw",
    bronze_root: str = "data/bronze",
):
    """扫描 raw 目录并批量摄取所有 CSV 文件"""
    raw_path = Path(raw_dir)
    if not raw_path.exists():
        print(f"[Bronze Ingest] Raw directory does not exist: {raw_dir}")
        return

    csv_files = list(raw_path.glob("*.csv"))
    if not csv_files:
        print(f"[Bronze Ingest] No CSV files found in {raw_dir}")
        return

    for csv_file in csv_files:
        table_name = csv_file.stem
        ingest_csv_to_bronze(
            spark=spark,
            csv_path=str(csv_file),
            table_name=table_name,
            bronze_root=bronze_root,
        )


def main():
    parser = argparse.ArgumentParser(description="Bronze Layer Ingestion CLI")
    parser.add_argument("--csv", type=str, help="Path to single CSV file")
    parser.add_argument("--table", type=str, help="Target table name")
    parser.add_argument("--raw-dir", type=str, default="data/raw", help="Directory containing raw CSVs")
    parser.add_argument("--bronze-root", type=str, default="data/bronze", help="Bronze root output path")
    args = parser.parse_args()

    spark = get_spark_session()
    try:
        if args.csv and args.table:
            ingest_csv_to_bronze(spark, args.csv, args.table, args.bronze_root)
        else:
            batch_ingest_raw(spark, args.raw_dir, args.bronze_root)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
