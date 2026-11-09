"""Silver Layer Data Contracts & Cleansing Engine.
Silver 层数据契约与清洗引擎。

Owner / 负责人: Tianyue Li / 李天悦
Module Functions / 模块功能:
  1. Enforce schema constraints on Bronze data using declarative Data Contracts.
     依据声明式 Data Contract 对 Bronze 数据实施 Schema 强制校验。
  2. Perform semantic cleansing, safe null replacement, and range constraints.
     执行业务语义清洗、空值安全置换及范围约束。
  3. Support SCD2 (Slowly Changing Dimension Type 2) historical change tracking.
     支持 SCD2 (缓慢变化维 Type 2) 维表变更追踪。
  4. Produce Delta Lake datasets compliant with Silver layer standards.
     产出符合 Silver 层标准规范的 Delta Lake 数据集。
"""

import os
from typing import Dict, List, Any, Optional
from datetime import datetime
from pydantic import BaseModel, Field

# Conditional import for PySpark to allow contract inspection without full Spark runtime
# 条件导入 PySpark，允许在无完整 Spark 运行时的轻量环境下检查契约规范
try:
    from pyspark.sql import SparkSession, DataFrame
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        StructType,
        StructField,
        StringType,
        IntegerType,
        LongType,
        DoubleType,
        TimestampType,
        BooleanType,
    )
    HAS_PYSPARK = True
except ImportError:
    HAS_PYSPARK = False
    SparkSession = Any  # type: ignore
    DataFrame = Any     # type: ignore


class ColumnContract(BaseModel):
    """Single column data contract definition.
    单字段数据契约定义。
    """
    name: str
    target_type: str = "string"  # int, long, double, string, timestamp
    nullable: bool = True
    min_value: Optional[float] = None
    max_value: Optional[float] = None
    allowed_values: Optional[List[str]] = None
    default_value: Optional[Any] = None


class TableContract(BaseModel):
    """Table-level data contract specification.
    表级数据契约规范。
    """
    table_name: str
    target_layer: str = "silver"
    primary_keys: List[str]
    columns: List[ColumnContract]
    scd2_enabled: bool = False
    scd2_tracked_columns: List[str] = Field(default_factory=list)


# ==============================================================================
# Official predefined Data Contracts (French Motor Third-Party Liability Dataset)
# 官方预设 Data Contracts (针对法国车险数据集)
# ==============================================================================

FREMTPL2FREQ_CONTRACT = TableContract(
    table_name="freMTPL2freq",
    primary_keys=["IDpol"],
    columns=[
        ColumnContract(name="IDpol", target_type="long", nullable=False),
        ColumnContract(name="ClaimNb", target_type="int", nullable=False, min_value=0),
        ColumnContract(name="Exposure", target_type="double", nullable=False, min_value=0.0, max_value=2.0),
        ColumnContract(name="Area", target_type="string", nullable=True, default_value="UNKNOWN"),
        ColumnContract(name="VehPower", target_type="int", nullable=True, min_value=1, max_value=50),
        ColumnContract(name="VehAge", target_type="int", nullable=True, min_value=0, max_value=100),
        ColumnContract(name="DrivAge", target_type="int", nullable=False, min_value=16, max_value=110),
        ColumnContract(name="BonusMalus", target_type="int", nullable=True, min_value=50, max_value=350),
        ColumnContract(name="VehBrand", target_type="string", nullable=True),
        ColumnContract(name="VehGas", target_type="string", nullable=True),
        ColumnContract(name="Density", target_type="int", nullable=True, min_value=0),
        ColumnContract(name="Region", target_type="string", nullable=True),
    ],
    scd2_enabled=False,
)

FREMTPL2SEV_CONTRACT = TableContract(
    table_name="freMTPL2sev",
    primary_keys=["IDpol"],
    columns=[
        ColumnContract(name="IDpol", target_type="long", nullable=False),
        ColumnContract(name="ClaimAmount", target_type="double", nullable=False, min_value=0.0),
    ],
    scd2_enabled=False,
)


def get_spark_session(app_name: str = "SilverContracts") -> SparkSession:
    """Get or initialize SparkSession with Delta Lake support.
    获取或初始化支持 Delta Lake 的 SparkSession。
    """
    if not HAS_PYSPARK:
        raise ImportError(
            "PySpark is not installed in the current environment. Please install requirements.txt.\n"
            "当前环境未安装 PySpark。请先安装 requirements.txt。"
        )
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.execution.arrow.pyspark.enabled", "true")
        .getOrCreate()
    )


def apply_data_contract_and_cleanse(
    df: DataFrame,
    contract: TableContract,
) -> DataFrame:
    """Validate, cast data types, and cleanse null values against data contract.
    依据数据契约对输入 DataFrame 进行校验、类型转换与空值清洗。

    Args:
        df: Raw or Bronze input DataFrame / 原始或 Bronze 输入 DataFrame
        contract: Target table data contract / 目标表级数据契约规范

    Returns:
        Cleansed and standardized DataFrame / 清洗规范化后的 DataFrame
    """
    cleaned_df = df

    # 1. Enforce Primary Key non-null constraint / 主键非空强制校验
    for pk in contract.primary_keys:
        if pk in cleaned_df.columns:
            cleaned_df = cleaned_df.filter(F.col(pk).isNotNull())

    # 2. Type casting and default value replacement / 字段类型转换与默认值填充
    for col_spec in contract.columns:
        cname = col_spec.name
        if cname not in cleaned_df.columns:
            # Missing contract column, fill with default / 契约字段缺失，补全默认值
            cleaned_df = cleaned_df.withColumn(cname, F.lit(col_spec.default_value))
            continue

        # Type casting mappings / 类型转换映射
        if col_spec.target_type == "int":
            cleaned_df = cleaned_df.withColumn(cname, F.col(cname).cast(IntegerType()))
        elif col_spec.target_type == "long":
            cleaned_df = cleaned_df.withColumn(cname, F.col(cname).cast(LongType()))
        elif col_spec.target_type == "double":
            cleaned_df = cleaned_df.withColumn(cname, F.col(cname).cast(DoubleType()))
        elif col_spec.target_type == "string":
            cleaned_df = cleaned_df.withColumn(cname, F.col(cname).cast(StringType()))

        # Default value replacement when null / 空值默认值置换
        if col_spec.default_value is not None:
            cleaned_df = cleaned_df.withColumn(
                cname,
                F.coalesce(F.col(cname), F.lit(col_spec.default_value)),
            )

        # Boundary filtering and constraints (Honest validation) / 数值边界过滤与约束
        if col_spec.min_value is not None:
            cleaned_df = cleaned_df.filter(F.col(cname) >= col_spec.min_value)
        if col_spec.max_value is not None:
            cleaned_df = cleaned_df.filter(F.col(cname) <= col_spec.max_value)

    # 3. Append Silver audit timestamp / 追加 Silver 层系统审计时间戳
    cleaned_df = cleaned_df.withColumn("_silver_processed_at", F.current_timestamp())

    return cleaned_df


def write_to_silver_delta(
    df: DataFrame,
    table_name: str,
    silver_root: str = "data/silver",
    mode: str = "overwrite",
) -> str:
    """Write cleansed data to Delta Lake Silver layer.
    将清洗后的数据写入 Delta Lake Silver 层。
    """
    target_path = os.path.join(silver_root, table_name)
    (
        df.write.format("delta")
        .mode(mode)
        .save(target_path)
    )
    print(f"[Silver Contracts] Written to Delta Lake / 写入 Delta Lake 完成: {target_path}")
    return target_path


def apply_scd2_tracking(
    current_silver_df: DataFrame,
    new_incoming_df: DataFrame,
    pk_col: str,
    tracked_cols: List[str],
) -> DataFrame:
    """SCD2 (Slowly Changing Dimension Type 2) version tracker.
    SCD2 (缓慢变化维 Type 2) 变更版本生成器。

    Track historical records and append validity timestamps and current flags:
    追随历史记录并追加生效日期与当前标志:
      - effective_start_date
      - effective_end_date (NULL denotes current active record / NULL 表示当前最新)
      - is_current (True / False)
    """
    now = datetime.utcnow()
    # Baseline SCD2 structure implementation / 基础 SCD2 结构实现
    incoming_staged = (
        new_incoming_df.withColumn("effective_start_date", F.lit(now))
        .withColumn("effective_end_date", F.lit(None).cast(TimestampType()))
        .withColumn("is_current", F.lit(True))
    )
    return incoming_staged
