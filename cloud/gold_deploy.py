"""Gold Layer Deployment and Quality Gate Dispatcher.
Gold 业务层自动化部署与质量门禁调度器。

Owner / 负责人: Tianyue Li / 李天悦
Module Function / 模块功能:
  - Enforce production deployment gates based on verification status and human approval.
    基于验证状态与人工审批实施生产部署门禁。
  - Deploy verified Job outputs (actuarial triangles, KPI aggregates) into Delta Lake Gold layer.
    将验证通过的作业产出 (精算三角形、KPI 宽表) 部署并发布至 Delta Lake Gold 层。
Contract Specification / 契约规范:
  - Gold storage path / Gold 存储路径: data/gold/<table_name>
  - Security Gate / 安全门禁: ONLY jobs with status 'Verified' or 'Tested' AND 'human_approval.approved == true'
    are permitted to write into Gold layer. Jobs marked 'Needs review' are strictly prohibited.
    仅允许状态为 'Verified' 或 'Tested' 且 'human_approval.approved == true' 的作业写入 Gold 生产层，严禁带病代码或 Needs review 注入。
"""

import os
import json
from typing import Dict, Any, List, Optional
from datetime import datetime

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


class DeploymentGateResult:
    """Quality gate validation result.
    质量门禁验证结果。
    """
    def __init__(self, job_id: str, allowed: bool, reason: str):
        self.job_id = job_id
        self.allowed = allowed
        self.reason = reason

    def __repr__(self) -> str:
        status_text = "APPROVED" if self.allowed else "REJECTED"
        return f"<GateResult [{status_text}] Job={self.job_id}: {self.reason}>"


def check_gold_deployment_gate(job_meta: Dict[str, Any]) -> DeploymentGateResult:
    """Validate whether a Job satisfies the Gold layer production deployment gate.
    校验某个作业是否满足 Gold 生产层部署门禁。

    Args:
        job_meta: Metadata dictionary from status_store.json / 来自 status_store.json 的作业元数据字典

    Returns:
        DeploymentGateResult with authorization status / 包含授权状态的门禁结果对象
    """
    job_id = job_meta.get("job_id", "UNKNOWN")
    status = job_meta.get("status", "Needs review")
    reconciliation_passed = job_meta.get("reconciliation_passed", False)
    discrepancies = job_meta.get("discrepancies", 999999)
    human_approval = job_meta.get("human_approval", {})
    approved = human_approval.get("approved", False)

    # 1. Check reconciliation pass / 校验对账是否通过
    if not reconciliation_passed or discrepancies != 0:
        return DeploymentGateResult(
            job_id=job_id,
            allowed=False,
            reason=f"Reconciliation check failed with {discrepancies} discrepancies / 对账校验未通过，存在差异数: {discrepancies}",
        )

    # 2. Check status level / 校验作业验证等级
    if status not in ["Verified", "Tested"]:
        return DeploymentGateResult(
            job_id=job_id,
            allowed=False,
            reason=f"Status is '{status}'. Only 'Verified' or 'Tested' are eligible for Gold layer / 状态为 '{status}'，仅允许 Verified 或 Tested 进入 Gold 层",
        )

    # 3. Check human approval sign-off / 校验人工审批签署
    if not approved:
        return DeploymentGateResult(
            job_id=job_id,
            allowed=False,
            reason="Missing human approval sign-off / 缺少人工审计签署",
        )

    reviewer = human_approval.get("reviewer", "N/A")
    return DeploymentGateResult(
        job_id=job_id,
        allowed=True,
        reason=f"Passed all safety gates. Approved by {reviewer} / 通过全部安全门禁，由审核人 {reviewer} 批准",
    )


def deploy_to_gold(
    spark: SparkSession,
    source_df: DataFrame,
    table_name: str,
    gold_root: str = "data/gold",
    mode: str = "overwrite",
) -> str:
    """Deploy validated dataset to Delta Lake Gold layer.
    将通过验证的数据集发布至 Delta Lake Gold 层。

    Args:
        spark: Active SparkSession / 活跃的 SparkSession 实例
        source_df: Source DataFrame / 源 DataFrame
        table_name: Target Gold table name / 目标 Gold 表名
        gold_root: Gold layer root directory / Gold 层根目录
        mode: Delta write mode / Delta 写入模式

    Returns:
        Target deployment path / 目标部署路径
    """
    target_path = os.path.join(gold_root, table_name)
    now = datetime.utcnow()

    # Append Gold production governance metadata
    # 追加 Gold 生产治理审计元数据
    df_gold = (
        source_df
        .withColumn("_gold_deployed_at", F.lit(now))
        .withColumn("_gold_tier_status", F.lit("PRODUCTION_VERIFIED"))
    )

    df_gold.write.format("delta").mode(mode).save(target_path)
    print(f"[Gold Deploy] Successfully deployed to {target_path} / 成功部署至 {target_path}")
    return target_path


def load_status_store(status_store_path: str = "ui/mock_data/status_store.json") -> List[Dict[str, Any]]:
    """Load job verification records from status store file.
    从状态存储文件中读取作业验证记录。
    """
    if not os.path.exists(status_store_path):
        return []
    with open(status_store_path, "r", encoding="utf-8") as f:
        data = json.load(f)
        return data.get("jobs", [])


def run_deployment_pipeline(
    status_store_path: str = "ui/mock_data/status_store.json",
    gold_root: str = "data/gold",
) -> Dict[str, Any]:
    """Execute batch deployment evaluation pipeline.
    执行批量部署评估流水线。
    """
    jobs = load_status_store(status_store_path)
    summary = {"evaluated": len(jobs), "approved": 0, "rejected": 0, "details": []}

    for job in jobs:
        gate_res = check_gold_deployment_gate(job)
        summary["details"].append(gate_res)
        if gate_res.allowed:
            summary["approved"] += 1
            print(f"[Gate APPROVED] Job {job.get('job_id')}: {gate_res.reason}")
        else:
            summary["rejected"] += 1
            print(f"[Gate REJECTED] Job {job.get('job_id')}: {gate_res.reason}")

    return summary


if __name__ == "__main__":
    # Test gate evaluation on default status store
    # 在默认状态存储上测试门禁评估
    print("Evaluating Gold deployment quality gates / 评估 Gold 部署质量门禁...")
    results = run_deployment_pipeline()
    print(f"Summary / 总结: Approved={results['approved']}, Rejected={results['rejected']}")

