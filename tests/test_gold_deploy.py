"""Unit Tests for Gold Layer Deployment, Scheduling & SLA Alerts.
Gold 业务层部署、调度与 SLA 告警单元测试套件。

Owner / 负责人: Tianyue Li / 李天悦
Test Objectives / 测试目标:
  - Verify that approved Verified/Tested jobs are admitted and deployed into the Gold layer.
    验证经审批的 Verified 与 Tested 作业正常通过准入并部署至 Gold 业务层。
  - Verify that unapproved or 'Needs review' jobs are strictly blocked with explicit logging.
    验证未审批或 'Needs review' 状态的作业被严密拦截并输出阻止原因。
  - Verify that SLA duration or row count violations accurately trigger alerts written to sla_alerts.json.
    验证 SLA 执行超时或行数不足能够准确触发告警并记录入 sla_alerts.json。
"""

import os
import json
import tempfile
import pytest
from cloud.gold_deploy import (
    evaluate_admission_gate,
    execute_gold_deployment,
    StatusStoreError,
    load_status_store,
)


def test_admission_gate_allows_approved_verified_and_tested():
    """Verify admission gate accepts only approved Verified and Tested jobs.
    验证准入安全门禁仅放行已获审批的 Verified 与 Tested 作业。
    """
    # 1. Approved Verified job / 已审批 Verified 作业
    job_verified = {
        "job_id": "J01",
        "status": "Verified",
        "reconciliation_passed": True,
        "human_approval": {"approved": True, "reviewer": "Tianyue Li"},
    }
    decision = evaluate_admission_gate(job_verified)
    assert decision.eligible is True
    assert "Ready for Gold deployment" in decision.reason

    # 2. Approved Tested job (AI-augmented with 100% parity) / 已审批 Tested 作业
    job_tested = {
        "job_id": "S01",
        "status": "Tested",
        "reconciliation_passed": True,
        "human_approval": {"approved": True, "reviewer": "Pin-Yun"},
    }
    decision = evaluate_admission_gate(job_tested)
    assert decision.eligible is True


def test_admission_gate_blocks_needs_review_and_unapproved():
    """Verify admission gate strictly blocks Needs review or unapproved jobs.
    验证准入安全门禁坚决拦截 Needs review 与未获人工签署的作业。
    """
    # 1. Needs review job / 待审作业 (即使标记 approved 也绝不允许进入 Gold)
    job_needs_review = {
        "job_id": "J09",
        "status": "Needs review",
        "reconciliation_passed": False,
        "human_approval": {"approved": False},
    }
    decision = evaluate_admission_gate(job_needs_review)
    assert decision.eligible is False
    assert "Needs review" in decision.reason

    # 2. Verified job without human sign-off / 缺少人工签署的 Verified 作业
    job_unapproved = {
        "job_id": "J02",
        "status": "Verified",
        "reconciliation_passed": True,
        "human_approval": {"approved": False},
    }
    decision = evaluate_admission_gate(job_unapproved)
    assert decision.eligible is False
    assert "Missing human-in-the-loop approval" in decision.reason


def test_gold_deployment_pipeline_and_sla_alerts():
    """Test full gold deployment execution and SLA breach detection.
    测试完整的 Gold 部署流水线执行与 SLA 违规告警捕获。
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        status_file = os.path.join(tmp_dir, "status_store.json")
        gold_dir = os.path.join(tmp_dir, "gold")

        # Mock status store with 1 approved job and 1 blocked job
        # 构造包含 1 个合规作业与 1 个待审拦截作业的模拟状态表
        mock_store = {
            "jobs": [
                {
                    "job_id": "J00_mock",
                    "status": "Verified",
                    "reconciliation_passed": True,
                    "human_approval": {"approved": True, "reviewer": "Tianyue Li"},
                },
                {
                    "job_id": "J09",
                    "status": "Needs review",
                    "reconciliation_passed": False,
                    "human_approval": {"approved": False},
                },
            ]
        }
        with open(status_file, "w", encoding="utf-8") as f:
            json.dump(mock_store, f)

        # Trigger deployment with intentionally stringent SLA (min_rows=999999 to force SLA alert)
        # 触发部署并故意设置极高的行数阈值 (min_rows=999999) 验证 SLA 告警逻辑
        summary = execute_gold_deployment(
            status_store_path=status_file,
            gold_root=gold_dir,
            max_duration_sec=30.0,
            min_rows=999999,
        )

        assert summary["deployed_count"] == 1
        assert summary["blocked_count"] == 1
        assert summary["sla_alerts_count"] == 1

        # Verify SLA alerts file contents / 验证 SLA 告警文件内容
        alerts_path = summary["alerts_file"]
        assert os.path.exists(alerts_path)
        with open(alerts_path, "r", encoding="utf-8") as f:
            alerts = json.load(f)
            assert len(alerts) == 1
            assert alerts[0]["job_id"] == "J00_mock"
            assert any("below minimum threshold" in r for r in alerts[0]["breach_reasons"])

        # Verify deployment audit log / 验证部署审计日志
        audit_path = summary["audit_file"]
        assert os.path.exists(audit_path)
        with open(audit_path, "r", encoding="utf-8") as f:
            audit = json.load(f)
            assert len(audit) == 2
            actions = {a["job_id"]: a["action"] for a in audit}
            assert actions["J00_mock"] == "DEPLOYED"
            assert actions["J09"] == "BLOCKED"


def test_missing_or_broken_status_table_is_an_error():
    """The gate must refuse to run without real job results; it never makes up a status table.
    没有真实作业结果时门禁必须拒绝运行，绝不虚构状态表。
    """
    with tempfile.TemporaryDirectory() as tmp_dir:
        status_file = os.path.join(tmp_dir, "status_store.json")
        with pytest.raises(StatusStoreError, match="engine export"):
            load_status_store(status_file)
        assert not os.path.exists(status_file)

        with open(status_file, "w", encoding="utf-8") as f:
            f.write("not json")
        with pytest.raises(StatusStoreError, match="not valid JSON"):
            load_status_store(status_file)

        with open(status_file, "w", encoding="utf-8") as f:
            json.dump({"metrics": {}}, f)
        with pytest.raises(StatusStoreError, match="jobs"):
            execute_gold_deployment(status_store_path=status_file, gold_root=os.path.join(tmp_dir, "gold"))
