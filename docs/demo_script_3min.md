# 3-Minute Video Demo Script & Storyboard
# 3 分钟评委演示视频分镜与全流程演说脚本

> **Challenge Track / 赛题:** Challenge 03: Agentic Data Platform Modernisation (Legacy ETL/SAS to Azure Lakehouse)  
> **Team / 战队:** UCD Team (Challenge 03)  
> **Speaker / 演说人:** Tianyue Li / 李天悦 (DevOps & Lakehouse Lead)  
> **Target Duration / 目标时长:** Exactly 180 seconds (3:00)  

---

## Storyboard Overview & Timeline / 分镜总览与时间线

```plaintext
[0:00 - 0:30] Part 1: The Hook & Core Doctrine ("AI Proposes, the Compiler Disposes")
[0:30 - 1:15] Part 2: Medallion Lakehouse & Quarantine DLQ (1,224 Anomalies Caught)
[1:15 - 2:15] Part 3: The J04 Semantic Trap, Failure Shrinking & Automated Self-Healing
[2:15 - 2:45] Part 4: Human-in-the-Loop Sign-off, Gold Deployment & SLA Alerts
[2:45 - 3:00] Part 5: 1,200-Job Enterprise ROI & Responsible AI Closing
```

---

### [0:00 - 0:30] Part 1: The Hook & Core Doctrine
**Visual on Screen / 画面**:
Show the High-Resolution Architecture Diagram (`docs/architecture_diagram.svg`) zooming in from Zone 1 to Zone 2, then cutting to the terminal showing `exl-migration-factory`.

**English Narration / 英文旁白**:
> "Modernizing 1,200 legacy T-SQL procedures and 400 SAS actuarial scripts to Azure Lakehouse sounds like an ideal job for Generative AI. But in insurance and banking, unchecked LLM hallucinations cause silent calculation errors and regulatory disaster.
> 
> Our team built the **EXL Migration Factory** under our team's First Principle: **'AI Proposes, the Compiler Disposes.'** 
> 70% of standard transformations are compiled deterministically with zero tokens via `sqlglot`. Complex logic routes to our audited, zero-PII LLM agent, but no code ever touches production without passing mathematical, record-for-record reconciliation."

**中文对齐要点**:
开篇抛出核心痛点：1,200 个旧存储过程与 400 个 SAS 精算脚本迁移若单纯依赖大模型会产生不可接受的幻觉与数值漂移。展示系统第一铁律：“AI 提议代码，编译器裁决生死”，70% 确定性 AST 编译零消耗 Token，30% 复杂逻辑进入零 PII 的受控 LLM 兜底回路。

---

### [0:30 - 1:15] Part 2: Medallion Lakehouse & Quarantine DLQ
**Visual on Screen / 画面**:
Switch to the terminal running `python cloud/silver_contracts.py`. Show the console output with the structured audit box: 678,013 rows ingested, 676,789 cleansed (99.82%), and 1,224 anomalous rows trapped into Quarantine. Show the generated Delta table in `data/quarantine/`.

**English Narration / 英文旁白**:
> "Watch our Lakehouse Medallion tier in action on the full 678,000-record French motor insurance dataset.
> 
> In the Bronze layer, raw CSVs are ingested append-only with immutable audit timestamps. 
> As data moves to the Silver layer, our declarative Data Contracts enforce strict schema casting, whitespace trimming, and legal driving age boundaries. 
> Notice this: 1,224 corrupt records with impossible exposure values greater than 1.0 were instantly trapped and routed into our **Quarantine Dead-Letter Queue** with failure tags like `ERR_EXPOSURE_EXCEEDS_MAX`. 
> Clean Silver data achieved a **99.82% purity rate**, guaranteeing that dirty data never pollutes downstream actuarial models."

**中文对齐要点**:
实机演示 Silver 数据契约引擎处理 67.8 万条真实数据。高亮展示 1,224 条异常记录被精准隔离至 Quarantine 死信队列（DLQ），下游 Silver 纯净度达 99.82%，杜绝脏数据污染下游精算宽表。

---

### [1:15 - 2:15] Part 3: The J04 Trap, Failure Shrinking & Self-Healing
**Visual on Screen / 画面**:
Open the Streamlit UI to Job **J04 (`driver_demographic_loss_ratio`)**. Click the "Diff & Reconciliation Report". Show the red failure: SQL Server produced `0` while Spark produced `0.0452`. Then show the **Failure Shrinker** tab compressing 48,210 mismatches into 2 counterexample rows. Trigger the self-healing button, show the prompt audit log fixing integer division via `CAST(ClaimNb AS FLOAT)`, and show GitHub Actions CI turning 100% green.

**English Narration / 英文旁白**:
> "Here is our crowning engineering achievement: our automated dual-track reconciliation engine catching subtle legacy semantic bugs.
> 
> In legacy job J04, SQL Server performed silent integer division—truncating loss ratios to zero. An unconstrained AI migration would silently replicate this or introduce arbitrary rounding. 
> Our reconciler ran a record-for-record full outer join against the local SQL Server baseline and caught 48,210 discrepancies. 
> Instead of overwhelming the LLM with massive logs, our **Failure Shrinker** extracted the two minimal reproducible counterexamples. 
> Armed with this exact root cause hint, our bounded self-healing agent corrected the SQL cast in attempt number two. 
> The reconciler rerun showed **0 discrepancies**, and the GitHub Actions CI pipeline instantly turned green!"

**中文对齐要点**:
演示最震撼的技术亮点：J04 隐式整型除法截断陷阱。对账引擎通过全外连接精确定位 48,210 条偏差；反例缩减器将其压缩为 2 条最小反例反馈给自愈引擎；在第 2 轮迭代中成功修正并在 CI 门禁中全绿通过！

---

### [2:15 - 2:45] Part 4: Human-in-the-Loop Sign-off, Gold Deployment & SLA
**Visual on Screen / 画面**:
In the Streamlit UI, show the "Human Approval" toggle. Click "Approve & Sign-off" on J04 as Lead Actuary. Switch to the terminal and run `python cloud/gold_deploy.py`. Show the admission gate deploying 10 approved jobs while strictly blocking J09 and S03 with `[BLOCKED]`. Show the SLA check passing with duration logs and `deployment_audit.json`.

**English Narration / 英文旁白**:
> "Even with a green reconciliation test, our governance requires a Human-in-the-Loop sign-off. In our Streamlit control plane, an actuary reviews the visual diff proof and signs off.
> 
> Next, our automated Gold Dispatcher executes. 
> Notice how jobs J09 and S03, which were flagged as 'Needs review' due to complex cursor logic, are strictly **[BLOCKED]** from entering the Gold layer. 
> For all approved jobs, production aggregate marts and actuarial loss triangles are deployed to Delta Lake under strict SLA monitoring—alerting us if execution exceeds 30 seconds or drops records. 
> Zero defective code can ever slip through."

**中文对齐要点**:
展示负责任 AI 的人工兜底闭环：精算专家在前端签署审批。随后运行 Gold 部署引擎，显式展示对 J09 与 S03 的安全拦截（`[BLOCKED]`），并展示带 30 秒超时监控的 SLA 自动化审计日志。

---

### [2:45 - 3:00] Part 5: 1,200-Job Enterprise ROI & Closing
**Visual on Screen / 画面**:
Switch to the Streamlit "Enterprise Modernisation & ROI" tab. Show the macroeconomic model extrapolating across 1,200 ETL + 400 SAS jobs: **83.3% automated pass rate, 76% cost savings, 14,000 engineering hours saved, 3.2-month migration timeline**. Cut back to the team slide.

**English Narration / 英文旁白**:
> "Extrapolated across an enterprise portfolio of 1,200 ETL and 400 SAS jobs, the EXL Migration Factory achieves an **83.3% automated migration pass rate**, saving over **14,000 engineering hours** and **76% of modernization costs**—compressing a multi-year risk into a 3-month certifiable migration.
> 
> Deterministic where possible, agentic where necessary, and verifiable always. Thank you."

**中文对齐要点**:
30 秒商业价值收尾：基于 12 个代表性样本宏观推演 1,200 个作业，实现 83.3% 自动化迁移率，节约 76% 成本与 14,000 人时。以“确定性优先、AI 兜底、数学级可验证”强力结语。

