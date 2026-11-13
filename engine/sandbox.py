"""Check and run generated PySpark code against in-memory tables.
檢查並執行產生的 PySpark 程式,輸入為記憶體中的表。

The same door is used for rule-generated and AI-generated code. The check is a
guard rail against accidents and obvious misuse (file access, process spawning),
not a hardened security boundary: code still runs inside this Python process.
規則產生與 AI 產生的程式走同一道門。這裡的檢查是防止意外與明顯濫用
(讀寫檔案、啟動行程)的護欄,不是嚴格的安全邊界:程式仍在本行程內執行。
"""
from __future__ import annotations

import ast
import atexit
import builtins
import datetime as dt
import itertools
import json
import shutil
import tempfile
import os
import sys
import threading
from decimal import Decimal
from pathlib import Path

from engine.model import Column, Table, decimal_parts, is_decimal

ALLOWED_IMPORTS = ("pyspark", "decimal", "datetime", "math", "functools", "itertools")
FORBIDDEN_NAMES = {
    "open", "exec", "eval", "compile", "__import__", "input", "globals", "locals",
    "getattr", "setattr", "delattr", "vars", "breakpoint", "exit", "quit",
}
# Attributes that reach files, the JVM or cluster settings.
# 會碰到檔案、JVM 或叢集設定的屬性。
FORBIDDEN_ATTRIBUTES = {"read", "write", "readStream", "writeStream", "sparkContext", "catalog", "newSession", "stop"}
_SAFE_BUILTINS = (
    "abs all any bool dict enumerate float int isinstance len list map max min print range reversed round set "
    "sorted str sum tuple zip filter None True False Exception ValueError TypeError KeyError"
).split()


class SandboxError(Exception):
    """Generated code was rejected or failed while running.
    產生的程式被拒絕,或在執行時失敗。
    """


def check_code(source: str) -> None:
    """Reject code that does more than transform DataFrames.
    拒絕做了「轉換 DataFrame」以外事情的程式。
    """
    try:
        tree = ast.parse(source)
    except SyntaxError as exc:
        raise SandboxError(f"not valid Python: {exc.msg} (line {exc.lineno})") from exc
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            modules = [alias.name for alias in node.names]
        elif isinstance(node, ast.ImportFrom):
            modules = [node.module or ""] if node.level == 0 else ["<relative import>"]
        else:
            modules = []
        for module in modules:
            if not any(module == root or module.startswith(root + ".") for root in ALLOWED_IMPORTS):
                raise SandboxError(f"import of {module!r} is not allowed (line {node.lineno})")
        if isinstance(node, ast.Name) and node.id in FORBIDDEN_NAMES:
            raise SandboxError(f"use of {node.id!r} is not allowed (line {node.lineno})")
        if isinstance(node, ast.Attribute) and (node.attr.startswith("_") or node.attr in FORBIDDEN_ATTRIBUTES):
            raise SandboxError(f"access to .{node.attr} is not allowed (line {node.lineno})")
    runs = [n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == "run"]
    if len(runs) != 1 or len(runs[0].args.args) != 2:
        raise SandboxError("the code must define exactly one function `run(spark, tables)`")


def get_spark():
    """Create (or reuse) a small local Spark session sized for a laptop.
    建立(或重用)一個適合筆電的小型本機 Spark session。
    """
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("migration-factory")
        .config("spark.driver.memory", "1g")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "false")
        # Keep decimal results exact instead of silently dropping digits; generated code relies on it.
        # 讓小數運算結果保持精確、不默默丟位數;產生的程式依賴此設定。
        .config("spark.sql.decimalOperations.allowPrecisionLoss", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def to_dataframe(spark, table: Table):
    from pyspark.sql import types as T

    def field_type(type_name: str):
        if is_decimal(type_name):
            return T.DecimalType(*decimal_parts(type_name))
        simple = {
            "int": T.IntegerType(), "bigint": T.LongType(), "float": T.DoubleType(),
            "string": T.StringType(), "bool": T.BooleanType(), "date": T.DateType(),
        }
        if type_name not in simple:
            raise SandboxError(f"unsupported column type {type_name!r}")
        return simple[type_name]

    schema = T.StructType([T.StructField(c.name, field_type(c.type), True) for c in table.columns])
    # Rows travel through a JSON-lines file read by the JVM. spark.createDataFrame would need
    # Spark's Python worker processes, which are unreliable on Windows.
    # 資料列經由 JSON-lines 檔交給 JVM 讀取。spark.createDataFrame 需要 Spark 的
    # Python 工作行程,而它在 Windows 上並不可靠。
    path = _TEMP_DIR / f"table_{next(_FILE_COUNTER)}.json"
    names = [json.dumps(c.name) for c in table.columns]
    with path.open("w", encoding="utf-8", newline="\n") as handle:
        for row in table.rows:
            handle.write("{" + ",".join(f"{n}:{_json_value(v)}" for n, v in zip(names, row)) + "}\n")
    return spark.read.schema(schema).json(str(path))


_TEMP_DIR = Path(tempfile.mkdtemp(prefix="migration_factory_"))
_FILE_COUNTER = itertools.count()
atexit.register(shutil.rmtree, _TEMP_DIR, ignore_errors=True)


def _json_value(value) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, Decimal):
        return format(value, "f")
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            raise SandboxError("NaN / infinity cannot be loaded as input data")
        return repr(value)
    if isinstance(value, int):
        return str(value)
    if isinstance(value, (dt.date, dt.datetime)):
        return json.dumps(value.isoformat())
    return json.dumps(str(value), ensure_ascii=False)


def from_dataframe(df) -> Table:
    from pyspark.sql import types as T

    columns = []
    for f in df.schema.fields:
        t = f.dataType
        if isinstance(t, T.DecimalType):
            name = f"decimal({t.precision},{t.scale})"
        elif isinstance(t, (T.IntegerType, T.ShortType, T.ByteType)):
            name = "int"
        elif isinstance(t, T.LongType):
            name = "bigint"
        elif isinstance(t, (T.DoubleType, T.FloatType)):
            name = "float"
        elif isinstance(t, T.StringType):
            name = "string"
        elif isinstance(t, T.BooleanType):
            name = "bool"
        elif isinstance(t, T.DateType):
            name = "date"
        else:
            raise SandboxError(f"output column {f.name!r} has unsupported type {t.simpleString()}")
        columns.append(Column(f.name, name))
    lowered = [c.name.lower() for c in columns]
    if len(set(lowered)) != len(lowered):
        raise SandboxError("the output has two columns with the same name")
    return Table(columns, [tuple(row) for row in df.collect()])


def run_code(source: str, spark, inputs: dict[str, Table], timeout_seconds: float = 180) -> Table:
    """Check the code, run `run(spark, tables)` on the inputs and collect the result.
    檢查程式後,以輸入表執行 `run(spark, tables)` 並收集結果。

    Raises SandboxError for rejected code, run-time errors, wrong return type or timeout.
    程式被拒、執行期錯誤、回傳型別不對或逾時,都會丟出 SandboxError。
    """
    check_code(source)
    from pyspark.sql import DataFrame

    def guarded_import(name, globals=None, locals=None, fromlist=(), level=0):
        if level != 0 or not any(name == root or name.startswith(root + ".") for root in ALLOWED_IMPORTS):
            raise ImportError(f"import of {name!r} is not allowed")
        return builtins.__import__(name, globals, locals, fromlist, level)

    safe_builtins = {name: getattr(builtins, name) for name in _SAFE_BUILTINS}
    safe_builtins["__import__"] = guarded_import
    namespace = {"__builtins__": safe_builtins, "__name__": "generated_job"}
    outcome: dict = {}

    def work():
        try:
            exec(builtins.compile(source, "<generated>", "exec"), namespace)
            result = namespace["run"](spark, {name: to_dataframe(spark, t) for name, t in inputs.items()})
            if not isinstance(result, DataFrame):
                raise SandboxError(f"run() returned {type(result).__name__}, expected a DataFrame")
            outcome["table"] = from_dataframe(result)
        except SandboxError as exc:
            outcome["error"] = exc
        except Exception as exc:  # generated code may fail in any way; report it, never crash the pipeline
            outcome["error"] = SandboxError(f"{type(exc).__name__}: {_first_lines(str(exc))}")

    thread = threading.Thread(target=work, daemon=True)
    thread.start()
    thread.join(timeout_seconds)
    if thread.is_alive():
        spark.sparkContext.cancelAllJobs()
        raise SandboxError(f"the code did not finish within {timeout_seconds:.0f} seconds")
    if "error" in outcome:
        raise outcome["error"]
    table = outcome["table"]
    table.synthetic = all(t.synthetic for t in inputs.values())
    return table


def _first_lines(text: str, limit: int = 600) -> str:
    # Spark errors carry long Java stack traces; keep the part a person can act on.
    # Spark 的錯誤常帶很長的 Java 堆疊;只保留人能據以處理的部分。
    return text.split("\n\tat ")[0].split("JVM stacktrace:")[0].strip()[:limit]

