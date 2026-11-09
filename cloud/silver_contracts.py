"""Silver Layer Data Contracts & Cleansing Engine.
Silver 层数据契约与清洗引擎。

Owner / 负责人: Tianyue Li / 李天悦
Module Function / 模块功能:
  1. Read Bronze tables from Delta format (data/bronze/{table_name}).
     从 Delta 格式读取 Bronze 层数据 (data/bronze/{table_name})。
  2. Support both local directory and cloud ADLS Gen2 storage paths via CLI argument (--cloud).
     支持本地目录与通过命令行参数 (--cloud) 指定的 Azure ADLS Gen2 云端存储路径。
  3. Enforce strict schema casting, critical not-null constraints, and domain range validation.
     强制执行严格的 Schema 类型转换、关键非空约束与业务数值范围规则。
  4. Save cleansed & compliant data to Silver layer (data/silver/{table_name}) in Delta Lake format.
     将清洗合规数据以 Delta Lake 格式覆写保存至 Silver 层 (data/silver/{table_name})。
  5. Divert failing records to Quarantine layer (data/quarantine/{table_name}_quarantine) with audit metadata:
     将校验未通过记录分流至 Quarantine 隔离区，并追加审计字段:
     - `_quarantine_reasons` (Array of failure tags / 违规标签数组)
     - `_quarantined_at` (Timestamp of quarantine event / 隔离时间戳)
  6. Compute and log comprehensive audit metrics (Total, Passed, Quarantined, Pass Rate, Top Violations).
     计算并打印全面审计指标（总量、合格数、隔离数、合格率、Top 3 违规原因）。
"""

import os
import sys
import argparse
from typing import Dict, List, Any, Tuple, Optional
from datetime import datetime, timezone

# Conditional PySpark import to support development across heterogeneous environments
# 条件导入 PySpark，兼容多端开发与轻量测试环境
try:
    from pyspark.sql import SparkSession, DataFrame, Column
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
        LongType,
        DecimalType,
        TimestampType,
        ArrayType,
    )
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    SparkSession = Any  # type: ignore
    DataFrame = Any     # type: ignore
    Column = Any        # type: ignore


# ==============================================================================
# Violation Tags & Reason Identifiers
# 违规标签与原因标识符定义
# ==============================================================================

REASON_IDPOL_NULL = "ERR_IDPOL_NULL"
REASON_EXPOSURE_NON_POSITIVE = "ERR_EXPOSURE_NON_POSITIVE"
REASON_EXPOSURE_EXCEEDS_MAX = "ERR_EXPOSURE_EXCEEDS_MAX"
REASON_DRIVAGE_UNDERAGE = "ERR_DRIVAGE_UNDERAGE"
REASON_CLAIMNB_NEGATIVE = "ERR_CLAIMNB_NEGATIVE"
REASON_BONUSMALUS_TOO_LOW = "ERR_BONUSMALUS_TOO_LOW"
REASON_CLAIMAMOUNT_NON_POSITIVE = "ERR_CLAIMAMOUNT_NON_POSITIVE"


# ==============================================================================
# Spark Session Factory
# Spark 会话构建工厂
# ==============================================================================

def get_spark_session(app_name: str = "SilverDataContracts", cloud_storage: bool = False) -> SparkSession:
    """Initialize SparkSession with Delta Lake and optional Azure ADLS Gen2 configurations.
    初始化支持 Delta Lake 及可选 Azure ADLS Gen2 配置的 SparkSession。
    """
    if not HAS_PYSPARK:
        raise ImportError(
            "PySpark is not installed in the current environment. Please install requirements.txt.\n"
            "当前环境未安装 PySpark。请先安装 requirements.txt。"
        )

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
    )

    if cloud_storage:
        # Azure Blob / ADLS Gen2 connector packages and options
        # Azure ADLS Gen2 连接器扩展包及相关配置
        builder = (
            builder
            .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.hadoop:hadoop-azure:3.3.4")
            .config("fs.azure.account.auth.type", "OAuth")
            .config("fs.azure.account.oauth.provider.type", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
        )
    else:
        builder = builder.config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0")

    return builder.getOrCreate()


# ==============================================================================
# PySpark Contract Enforcement: freMTPL2freq
# PySpark 契约强制执行: freMTPL2freq
# ==============================================================================

def validate_and_cleanse_freq_spark(df_raw: DataFrame) -> Tuple[DataFrame, DataFrame, Dict[str, Any]]:
    """Enforce data contracts and partition freMTPL2freq into Silver and Quarantine streams.
    执行数据契约并将 freMTPL2freq 数据集划分为 Silver 优质流与 Quarantine 隔离流。

    Args:
        df_raw: Raw Bronze DataFrame / 原始 Bronze DataFrame

    Returns:
        Tuple of (df_silver, df_quarantine, audit_metrics_dict)
        元组包含 (Silver 清洗 DataFrame, Quarantine 隔离 DataFrame, 审计指标字典)
    """
    # 1. Schema casting and standardization / 字段类型转换与标准化清洗
    df_staged = (
        df_raw
        .withColumn("IDpol", F.col("IDpol").cast(LongType()))
        .withColumn("ClaimNb", F.col("ClaimNb").cast(IntegerType()))
        .withColumn("Exposure", F.col("Exposure").cast(DecimalType(10, 4)))
        .withColumn("VehPower", F.col("VehPower").cast(IntegerType()))
        .withColumn("VehAge", F.col("VehAge").cast(IntegerType()))
        .withColumn("DrivAge", F.col("DrivAge").cast(IntegerType()))
        .withColumn("BonusMalus", F.col("BonusMalus").cast(IntegerType()))
        # Categorical string standardization: trim, uppercase, and null replacement
        # 分类字符串清洗：去前后空格、大写转换、空值安全置换
        .withColumn("VehBrand", F.upper(F.trim(F.col("VehBrand").cast(StringType()))))
        .withColumn("VehGas", F.upper(F.trim(F.col("VehGas").cast(StringType()))))
        .withColumn(
            "Area",
            F.when(
                (F.col("Area").isNull()) | (F.trim(F.col("Area").cast(StringType())) == ""),
                F.lit("UNKNOWN")
            ).otherwise(F.upper(F.trim(F.col("Area").cast(StringType()))))
        )
        .withColumn(
            "Region",
            F.when(
                (F.col("Region").isNull()) | (F.trim(F.col("Region").cast(StringType())) == ""),
                F.lit("UNKNOWN")
            ).otherwise(F.upper(F.trim(F.col("Region").cast(StringType()))))
        )
    )

    # 2. Rule evaluation and quarantine reasons aggregation
    # 规则评估与隔离违规原因收集
    reason_conditions = [
        # Critical Not-Null: IDpol must NOT be NULL / 关键主键非空规则
        F.when(F.col("IDpol").isNull(), F.lit(REASON_IDPOL_NULL)),
        # Range rules: Exposure > 0 / 承保期大于 0
        F.when((F.col("Exposure").isNull()) | (F.col("Exposure") <= 0), F.lit(REASON_EXPOSURE_NON_POSITIVE)),
        # Range rules: Exposure <= 1.0 / 承保期上限不超过 1.0
        F.when(F.col("Exposure") > 1.0, F.lit(REASON_EXPOSURE_EXCEEDS_MAX)),
        # Legal driving age: DrivAge >= 18 / 法定驾驶年龄下限
        F.when((F.col("DrivAge").isNull()) | (F.col("DrivAge") < 18), F.lit(REASON_DRIVAGE_UNDERAGE)),
        # Claim count non-negative: ClaimNb >= 0 / 出险次数非负
        F.when((F.col("ClaimNb").isNull()) | (F.col("ClaimNb") < 0), F.lit(REASON_CLAIMNB_NEGATIVE)),
        # Risk factor lower bound: BonusMalus >= 50 / 奖惩系数下限
        F.when((F.col("BonusMalus").isNull()) | (F.col("BonusMalus") < 50), F.lit(REASON_BONUSMALUS_TOO_LOW)),
    ]

    # Filter out null tags to construct active quarantine reasons array
    # 过滤空标签构造当前记录的有效违规原因数组
    df_evaluated = df_staged.withColumn(
        "_quarantine_reasons",
        F.filter(F.array(*reason_conditions), lambda x: x.isNotNull())
    )

    # 3. Partitioning: Silver (0 reasons) vs Quarantine (>0 reasons)
    # 划分：Silver 层（0 违规）与 Quarantine 隔离区（>=1 违规）
    is_valid = F.size(F.col("_quarantine_reasons")) == 0

    df_silver = (
        df_evaluated
        .filter(is_valid)
        .drop("_quarantine_reasons")
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    df_quarantine = (
        df_evaluated
        .filter(~is_valid)
        .withColumn("_quarantined_at", F.current_timestamp())
    )

    # 4. Audit metrics calculation / 审计度量指标计算
    total_rows = df_evaluated.count()
    silver_count = df_silver.count()
    quarantine_count = df_quarantine.count()
    pass_rate = (silver_count / total_rows * 100.0) if total_rows > 0 else 0.0

    # Extract Top 3 failure reasons / 提取前三大违规成因
    top_violations = []
    if quarantine_count > 0:
        top_violations = (
            df_quarantine
            .select(F.explode("_quarantine_reasons").alias("reason"))
            .groupBy("reason")
            .count()
            .orderBy(F.col("count").desc())
            .limit(3)
            .collect()
        )
        top_violations = [(row["reason"], row["count"]) for row in top_violations]

    metrics = {
        "table_name": "freMTPL2freq",
        "total_rows": total_rows,
        "silver_count": silver_count,
        "quarantine_count": quarantine_count,
        "pass_rate_pct": round(pass_rate, 2),
        "top_violations": top_violations,
    }

    return df_silver, df_quarantine, metrics


# ==============================================================================
# PySpark Contract Enforcement: freMTPL2sev
# PySpark 契约强制执行: freMTPL2sev
# ==============================================================================

def validate_and_cleanse_sev_spark(df_raw: DataFrame) -> Tuple[DataFrame, DataFrame, Dict[str, Any]]:
    """Enforce data contracts and partition freMTPL2sev into Silver and Quarantine streams.
    执行数据契约并将 freMTPL2sev 数据集划分为 Silver 优质流与 Quarantine 隔离流。

    Args:
        df_raw: Raw Bronze DataFrame / 原始 Bronze DataFrame

    Returns:
        Tuple of (df_silver, df_quarantine, audit_metrics_dict)
        元组包含 (Silver 清洗 DataFrame, Quarantine 隔离 DataFrame, 审计指标字典)
    """
    # 1. Schema casting / 字段类型转换
    df_staged = (
        df_raw
        .withColumn("IDpol", F.col("IDpol").cast(LongType()))
        .withColumn("ClaimAmount", F.col("ClaimAmount").cast(DecimalType(12, 2)))
    )

    # 2. Rule evaluation / 规则评估
    reason_conditions = [
        # Critical Not-Null: IDpol must NOT be NULL / 关键主键非空规则
        F.when(F.col("IDpol").isNull(), F.lit(REASON_IDPOL_NULL)),
        # Range rules: ClaimAmount > 0 / 赔付金额必须严格大于 0
        F.when((F.col("ClaimAmount").isNull()) | (F.col("ClaimAmount") <= 0), F.lit(REASON_CLAIMAMOUNT_NON_POSITIVE)),
    ]

    df_evaluated = df_staged.withColumn(
        "_quarantine_reasons",
        F.filter(F.array(*reason_conditions), lambda x: x.isNotNull())
    )

    # 3. Partitioning: Silver vs Quarantine / 划分 Silver 与 Quarantine
    is_valid = F.size(F.col("_quarantine_reasons")) == 0

    df_silver = (
        df_evaluated
        .filter(is_valid)
        .drop("_quarantine_reasons")
        .withColumn("_silver_processed_at", F.current_timestamp())
    )

    df_quarantine = (
        df_evaluated
        .filter(~is_valid)
        .withColumn("_quarantined_at", F.current_timestamp())
    )

    # 4. Audit metrics calculation / 审计度量指标计算
    total_rows = df_evaluated.count()
    silver_count = df_silver.count()
    quarantine_count = df_quarantine.count()
    pass_rate = (silver_count / total_rows * 100.0) if total_rows > 0 else 0.0

    top_violations = []
    if quarantine_count > 0:
        top_violations = (
            df_quarantine
            .select(F.explode("_quarantine_reasons").alias("reason"))
            .groupBy("reason")
            .count()
            .orderBy(F.col("count").desc())
            .limit(3)
            .collect()
        )
        top_violations = [(row["reason"], row["count"]) for row in top_violations]

    metrics = {
        "table_name": "freMTPL2sev",
        "total_rows": total_rows,
        "silver_count": silver_count,
        "quarantine_count": quarantine_count,
        "pass_rate_pct": round(pass_rate, 2),
        "top_violations": top_violations,
    }

    return df_silver, df_quarantine, metrics


# ==============================================================================
# Lightweight Python/Pandas Fallback (for non-JVM / local test sandbox)
# 本地轻量级 Python/Pandas 降级逻辑 (用于无 JVM/本地沙盒测试)
# ==============================================================================

def validate_and_cleanse_freq_pandas(df_raw_dict_or_df: Any) -> Tuple[Any, Any, Dict[str, Any]]:
    """Cleanse freMTPL2freq using pure pandas logic as local non-JVM fallback.
    使用纯 Pandas 逻辑清洗 freMTPL2freq 作为本地无 JVM 环境的平滑降级。
    """
    import pandas as pd
    from decimal import Decimal

    df = pd.DataFrame(df_raw_dict_or_df).copy()

    # Pre-clean string columns
    for c in ["VehBrand", "VehGas"]:
        if c in df.columns:
            df[c] = df[c].astype(str).str.strip().str.upper()

    for c in ["Area", "Region"]:
        if c in df.columns:
            df[c] = df[c].fillna("UNKNOWN").astype(str).str.strip().str.upper()
            df[c] = df[c].replace("", "UNKNOWN")

    # Evaluate quarantine rules
    quarantine_reasons = []
    for idx, row in df.iterrows():
        reasons = []
        # IDpol not null
        if pd.isna(row.get("IDpol")) or row.get("IDpol") is None:
            reasons.append(REASON_IDPOL_NULL)
        # Exposure range
        exp = row.get("Exposure")
        if pd.isna(exp) or float(exp) <= 0:
            reasons.append(REASON_EXPOSURE_NON_POSITIVE)
        elif float(exp) > 1.0:
            reasons.append(REASON_EXPOSURE_EXCEEDS_MAX)
        # DrivAge >= 18
        age = row.get("DrivAge")
        if pd.isna(age) or int(age) < 18:
            reasons.append(REASON_DRIVAGE_UNDERAGE)
        # ClaimNb >= 0
        claims = row.get("ClaimNb")
        if pd.isna(claims) or int(claims) < 0:
            reasons.append(REASON_CLAIMNB_NEGATIVE)
        # BonusMalus >= 50
        bm = row.get("BonusMalus")
        if pd.isna(bm) or int(bm) < 50:
            reasons.append(REASON_BONUSMALUS_TOO_LOW)

        quarantine_reasons.append(reasons)

    df["_quarantine_reasons"] = quarantine_reasons
    mask_valid = df["_quarantine_reasons"].apply(len) == 0

    df_silver = df[mask_valid].drop(columns=["_quarantine_reasons"]).copy()
    now_str = datetime.now(timezone.utc).isoformat()
    df_silver["_silver_processed_at"] = now_str

    df_quarantine = df[~mask_valid].copy()
    df_quarantine["_quarantined_at"] = now_str

    total_rows = len(df)
    silver_count = len(df_silver)
    quarantine_count = len(df_quarantine)
    pass_rate = (silver_count / total_rows * 100.0) if total_rows > 0 else 0.0

    # Top violations
    all_reasons = [r for sub in df_quarantine["_quarantine_reasons"] for r in sub]
    reason_series = pd.Series(all_reasons).value_counts().head(3)
    top_violations = list(reason_series.items())

    metrics = {
        "table_name": "freMTPL2freq",
        "total_rows": total_rows,
        "silver_count": silver_count,
        "quarantine_count": quarantine_count,
        "pass_rate_pct": round(pass_rate, 2),
        "top_violations": top_violations,
    }

    return df_silver, df_quarantine, metrics


def validate_and_cleanse_sev_pandas(df_raw_dict_or_df: Any) -> Tuple[Any, Any, Dict[str, Any]]:
    """Cleanse freMTPL2sev using pure pandas logic as local non-JVM fallback.
    使用纯 Pandas 逻辑清洗 freMTPL2sev 作为本地无 JVM 环境的平滑降级。
    """
    import pandas as pd

    df = pd.DataFrame(df_raw_dict_or_df).copy()

    quarantine_reasons = []
    for idx, row in df.iterrows():
        reasons = []
        if pd.isna(row.get("IDpol")) or row.get("IDpol") is None:
            reasons.append(REASON_IDPOL_NULL)
        amt = row.get("ClaimAmount")
        if pd.isna(amt) or float(amt) <= 0:
            reasons.append(REASON_CLAIMAMOUNT_NON_POSITIVE)
        quarantine_reasons.append(reasons)

    df["_quarantine_reasons"] = quarantine_reasons
    mask_valid = df["_quarantine_reasons"].apply(len) == 0

    df_silver = df[mask_valid].drop(columns=["_quarantine_reasons"]).copy()
    now_str = datetime.now(timezone.utc).isoformat()
    df_silver["_silver_processed_at"] = now_str

    df_quarantine = df[~mask_valid].copy()
    df_quarantine["_quarantined_at"] = now_str

    total_rows = len(df)
    silver_count = len(df_silver)
    quarantine_count = len(df_quarantine)
    pass_rate = (silver_count / total_rows * 100.0) if total_rows > 0 else 0.0

    all_reasons = [r for sub in df_quarantine["_quarantine_reasons"] for r in sub]
    reason_series = pd.Series(all_reasons).value_counts().head(3)
    top_violations = list(reason_series.items())

    metrics = {
        "table_name": "freMTPL2sev",
        "total_rows": total_rows,
        "silver_count": silver_count,
        "quarantine_count": quarantine_count,
        "pass_rate_pct": round(pass_rate, 2),
        "top_violations": top_violations,
    }

    return df_silver, df_quarantine, metrics


# ==============================================================================
# Console Audit Logger
# 控制台审计日志打印器
# ==============================================================================

def print_audit_summary(metrics: Dict[str, Any]) -> None:
    """Print structured audit summary to console.
    向控制台打印结构化审计总结。
    """
    tbl = metrics.get("table_name", "Unknown")
    total = metrics.get("total_rows", 0)
    silver = metrics.get("silver_count", 0)
    quarantine = metrics.get("quarantine_count", 0)
    rate = metrics.get("pass_rate_pct", 0.0)
    top_v = metrics.get("top_violations", [])

    print("\n" + "=" * 60)
    print(f"  DATA CONTRACT AUDIT REPORT: {tbl}")
    print("=" * 60)
    print(f"  Total Processed Rows / 处理总行数   : {total:,}")
    print(f"  Silver Passed Rows / 清洗通过行数   : {silver:,}")
    print(f"  Quarantined Rows / 隔离阻断行数     : {quarantine:,}")
    print(f"  Pass Rate / 合格率                 : {rate:.2f}%")
    print("-" * 60)
    print("  Top Violation Reasons / 前三大违规成因:")
    if top_v:
        for idx, (reason, cnt) in enumerate(top_v, 1):
            print(f"    {idx}. {reason:<30}: {cnt:,} records")
    else:
        print("    None (100% compliant / 全部合规)")
    print("=" * 60 + "\n")


# ==============================================================================
# Main Pipeline Dispatcher
# 主处理调度管道
# ==============================================================================

def run_silver_contracts_pipeline(
    bronze_root: str = "data/bronze",
    silver_root: str = "data/silver",
    quarantine_root: str = "data/quarantine",
    cloud: bool = False,
    cloud_adls_prefix: Optional[str] = None,
) -> None:
    """Execute end-to-end Silver data contracts validation and routing pipeline.
    执行端到端 Silver 数据契约验证与隔离路由流水线。
    """
    # Adjust paths if cloud ADLS Gen2 prefix is provided
    # 若提供了云端 ADLS Gen2 前缀，自动组装云端存储路径
    if cloud and cloud_adls_prefix:
        bronze_root = f"{cloud_adls_prefix.rstrip('/')}/bronze"
        silver_root = f"{cloud_adls_prefix.rstrip('/')}/silver"
        quarantine_root = f"{cloud_adls_prefix.rstrip('/')}/quarantine"
        print(f"[Cloud Mode] Using ADLS Gen2 root: {cloud_adls_prefix}")

    spark = None
    if HAS_PYSPARK:
        try:
            spark = get_spark_session(cloud_storage=cloud)
            print("[Runtime] Connected to PySpark Delta Lake runtime / 已连接至 PySpark Delta Lake 运行时")
        except Exception as e:
            print(f"[Runtime Notice] PySpark initialization skipped ({e}). Falling back to local mode.")

    # Process freMTPL2freq
    print("\n>>> Processing freMTPL2freq contracts...")
    freq_source = os.path.join(bronze_root, "freMTPL2freq")
    freq_silver = os.path.join(silver_root, "freMTPL2freq")
    freq_quarantine = os.path.join(quarantine_root, "freMTPL2freq_quarantine")

    if spark:
        # Read from Bronze Delta
        df_freq_raw = spark.read.format("delta").load(freq_source)
        df_silver, df_quarantine, freq_metrics = validate_and_cleanse_freq_spark(df_freq_raw)

        # Write to Silver (mode: overwrite)
        df_silver.write.format("delta").mode("overwrite").save(freq_silver)
        # Write to Quarantine (mode: overwrite)
        df_quarantine.write.format("delta").mode("overwrite").save(freq_quarantine)
        print_audit_summary(freq_metrics)
    else:
        # Local Fallback using Pandas
        import pandas as pd
        # Support either csv in fallback bronze or raw source
        csv_path = os.path.join(freq_source, "freMTPL2freq_bronze.csv")
        if not os.path.exists(csv_path):
            csv_path = "data/raw/freMTPL2freq.csv"

        df_raw = pd.read_csv(csv_path)
        df_silver, df_quarantine, freq_metrics = validate_and_cleanse_freq_pandas(df_raw)

        os.makedirs(freq_silver, exist_ok=True)
        os.makedirs(freq_quarantine, exist_ok=True)
        df_silver.to_csv(os.path.join(freq_silver, "freMTPL2freq_silver.csv"), index=False)
        df_quarantine.to_csv(os.path.join(freq_quarantine, "freMTPL2freq_quarantine.csv"), index=False)
        print_audit_summary(freq_metrics)

    # Process freMTPL2sev
    print("\n>>> Processing freMTPL2sev contracts...")
    sev_source = os.path.join(bronze_root, "freMTPL2sev")
    sev_silver = os.path.join(silver_root, "freMTPL2sev")
    sev_quarantine = os.path.join(quarantine_root, "freMTPL2sev_quarantine")

    if spark:
        df_sev_raw = spark.read.format("delta").load(sev_source)
        df_silver_sev, df_quarantine_sev, sev_metrics = validate_and_cleanse_sev_spark(df_sev_raw)

        df_silver_sev.write.format("delta").mode("overwrite").save(sev_silver)
        df_quarantine_sev.write.format("delta").mode("overwrite").save(sev_quarantine)
        print_audit_summary(sev_metrics)
    else:
        import pandas as pd
        csv_path = os.path.join(sev_source, "freMTPL2sev_bronze.csv")
        if not os.path.exists(csv_path):
            csv_path = "data/raw/freMTPL2sev.csv"

        df_raw_sev = pd.read_csv(csv_path)
        df_silver_sev, df_quarantine_sev, sev_metrics = validate_and_cleanse_sev_pandas(df_raw_sev)

        os.makedirs(sev_silver, exist_ok=True)
        os.makedirs(sev_quarantine, exist_ok=True)
        df_silver_sev.to_csv(os.path.join(sev_silver, "freMTPL2sev_silver.csv"), index=False)
        df_quarantine_sev.to_csv(os.path.join(sev_quarantine, "freMTPL2sev_quarantine.csv"), index=False)
        print_audit_summary(sev_metrics)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Silver Layer Data Contracts Pipeline / Silver 层数据契约流水线")
    parser.add_argument("--bronze-root", type=str, default="data/bronze", help="Bronze storage path / Bronze 路径")
    parser.add_argument("--silver-root", type=str, default="data/silver", help="Silver storage path / Silver 路径")
    parser.add_argument("--quarantine-root", type=str, default="data/quarantine", help="Quarantine storage path / 隔离区路径")
    parser.add_argument("--cloud", action="store_true", help="Enable Azure ADLS Gen2 cloud mode / 启用 ADLS Gen2 云端模式")
    parser.add_argument("--cloud-prefix", type=str, default=None, help="ADLS Gen2 URI (abfss://...) / 云端存储前缀")
    args = parser.parse_args()

    run_silver_contracts_pipeline(
        bronze_root=args.bronze_root,
        silver_root=args.silver_root,
        quarantine_root=args.quarantine_root,
        cloud=args.cloud,
        cloud_adls_prefix=args.cloud_prefix,
    )
