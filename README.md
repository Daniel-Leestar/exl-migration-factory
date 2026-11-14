# EXL Migration Factory (Challenge 03: Agentic Data Platform Modernisation)
# EXL 自动化迁移工厂 (选题目三：智能数据平台现代化)

> **EXL & UCD Data & AI Hackathon 2026**  
> **Challenge Track / 参赛赛题:** Legacy ETL/SAS to Azure Lakehouse (遗留 ETL/SAS 向现代湖仓架构自动化迁移)  
> **Team / 参赛战队:** UCD Team (Challenge 03)  
> **Project Period / 冲刺周期:** 2026/11/09 - 2026/11/13  

---

## 1. Core Philosophy / 核心设计理念

> ### **[Team First Principle: AI Proposes, the Compiler Disposes]**
> Large Language Models (LLMs) never have the final say on production migration. LLM outputs must be either syntactically verified Intermediate Representations (IR) conforming to strict schemas, or PySpark scripts that pass row-by-row reconciliation against ground-truth outputs. Jobs failing automated reconciliation are permitted a maximum of 3 self-healing attempts before being degraded to `Needs review`. No defective code is ever permitted into the Gold production layer.
>
> ### **【团队研发第一铁律：AI Proposes, the Compiler Disposes】**
> 大语言模型（LLM）永远不拥有代码与迁移的最终决定权。LLM 产出要么是经过语法检查并符合 Schema 约束的结构化 IR，要么是必须通过严格对账测试的 PySpark 脚本。未能通过自动化对账的作业最多允许自愈修正 3 次，逾期直接降级为 Needs review，绝不允许任何带病代码注入 Gold 生产层。

---

## 2. Team Members & Responsibilities / 团队成员与职责分工

- **Tianyue Li / 李天悦**:
  - Lakehouse architecture design & CI/CD automation pipeline.
    湖仓架构设计与 CI/CD 自动化流水线。
  - Bronze raw layer ingestion, Silver data contracts, and Gold production deployment governance.
    Bronze 原始层摄取、Silver 数据契约引擎以及 Gold 生产部署门禁治理。
- **Pin-Yun Kuo / 郭小妘**:
  - Migration compiler kernel (`sqlglot` AST parsing & IR Schema v0).
    迁移编译器核心引擎（`sqlglot` 语法树解析与 IR 结构定义）。
  - Deterministic PySpark codegen, row-by-row reconciliation engine, and failure shrinking.
    确定性代码生成器、逐行对账引擎与反例缩减器。
- **Neeraj**:
  - SQL Server benchmark baseline execution & ground-truth parquet generation (`oracle_output.parquet`).
    本地 SQL Server 基准测试环境搭建与权威标准答案导出。
  - Streamlit governance workbench & interactive executive UI.
    Streamlit 交互式治理工作台与端到端血缘大屏。

---

## 3. Directory Structure Topology / 代码仓库目录拓扑

```plaintext
exl-migration-factory/
├── .github/
│   └── workflows/
│       └── ci.yml               # [EN] GitHub Actions CI pipeline with reconciliation gates
│                                # [CN] GitHub Actions 自动化对账 CI 门禁流水线
├── cloud/                       # [EN] Lakehouse storage & deployment tier
│                                # [CN] 湖仓架构与部署调度层
│   ├── bronze_ingest.py         # [EN] Raw CSV -> Delta Lake (Bronze ingestion)
│   │                            # [CN] CSV 到 Delta Lake 原始层摄取管道
│   ├── silver_contracts.py      # [EN] Data Contracts enforcement & semantic cleansing
│   │                            # [CN] 声明式数据契约验证与清洗引擎
│   └── gold_deploy.py           # [EN] Production quality gate dispatcher for Gold layer
│                                # [CN] 验证通过 Job 的 Gold 生产层自动化发布与门禁
├── jobs/                        # [EN] 12 migration job packages (J01~J09, S01~S03)
│                                # [CN] 12 个迁移作业独立包
│   ├── J01/                     # [EN] Self-contained job package triplet
│   │                            # [CN] 独立作业三元组 (legacy.sql, meta.yaml, oracle_output.parquet)
│   │   ├── legacy.sql           # [EN] Unaltered legacy T-SQL code / 原始遗留 SQL
│   │   ├── meta.yaml            # [EN] Job metadata & reconciliation schema / 作业元数据与对账声明
│   │   └── oracle_output.parquet# [EN] Authoritative benchmark ground truth / 旧系统权威标准答案
│   ├── ...                      # J02 to J09 (T-SQL)
│   └── S01/ ... S03/            # S01 to S03 (SAS Actuarial scripts / SAS 精算作业)
├── engine/                      # [EN] Migration Factory compiler kernel
│                                # [CN] 迁移工厂核心编译器
│   ├── parser.py                # [EN] sqlglot T-SQL AST parser / 语法树解析器
│   ├── ir_schema.py             # [EN] Intermediate Representation Pydantic models / IR 规范定义
│   ├── codegen.py               # [EN] Deterministic AST/IR to PySpark generator / 确定性代码生成器
│   ├── llm_fallback.py          # [EN] Audited LLM fallback translator / 审计追踪 LLM 兜底翻译器
│   ├── reconciler.py            # [EN] Row-by-row reconciliation engine / 逐行全外连接对账引擎
│   └── shrinker.py              # [EN] Minimal reproducible counterexample shrinker / 失败测试反例缩减器
├── ui/                          # [EN] Streamlit governance workbench & UI
│                                # [CN] Streamlit 交互与治理工作台
│   ├── app.py                   # [EN] Streamlit entrypoint / 主入口
│   ├── mock_data/               # [EN] Decoupled state stubs (status_store.json) / 状态存储存根
│   └── pages/                   # [EN] Overview, Lineage, Reconciliation & Business tabs / 大屏与详情页面
├── data/                        # [EN] Lakehouse physical data (Git ignored)
│                                # [CN] 湖仓物理存储目录 (Git 忽略，仅存样例)
│   ├── raw/                     # [EN] Raw datasets (freMTPL2freq.csv, freMTPL2sev.csv)
│   ├── bronze/                  # [EN] Delta Lake Bronze raw layer
│   ├── silver/                  # [EN] Delta Lake Silver cleansed layer
│   └── gold/                    # [EN] Delta Lake Gold business layer
├── docs/                        # [EN] Official hackathon deliverables
│                                # [CN] 参赛终验交付物
│   ├── architecture_diagram.png # [EN] Single-page system architecture diagram / 单页系统架构图
│   └── responsible_ai_note.pdf  # [EN] Single-page Responsible AI declaration / 单页 RAI 声明文档
├── tests/                       # [EN] Automated test suite / 自动化测试套件
├── requirements.txt             # [EN] Pinned global dependency manifest / 依赖锁定文件
└── README.md                    # [EN] Evaluation & reproduction guide / 评委复现指南
```

---

## 4. Medallion Lakehouse Architecture / 奖牌湖仓分层物理契约

| Layer / 分层 | Storage & Format / 存储与格式 | Data Contract Specification / 契约规范 | Owner / 负责人 |
| :--- | :--- | :--- | :--- |
| **Bronze (原始层)** | Delta Lake<br>`data/bronze/` | Preserves raw schema. Strictly appends `_ingested_at` and `_source_file` audit columns. Enforces append-only.<br>保留原始结构，追加审计元数据，禁止修改内容，只允许追加。 | Tianyue Li / 李天悦 |
| **Silver (清洗层)** | Delta Lake<br>`data/silver/` | Schema validation, type casting, safe null replacement, and SCD2 change tracking.<br>实施 Schema 强制验证、类型清洗、空值置换，支持 SCD2 维表追踪。 | Pin-Yun & Neeraj |
| **Gold (业务层)** | Delta Lake<br>`data/gold/` | Production aggregate tables and actuarial triangles. ONLY accessible by `Verified`/`Tested` jobs with human approval sign-off.<br>精算三角形与 KPI 宽表；仅允许状态为 Verified/Tested 且经人工审批的作业写入。 | Tianyue Li / 李天悦 |

---

## 5. Quickstart & Reproduction Guide / 快速开始与评委复现指南

### Step 1: Environment Setup / 环境准备与依赖安装
Recommended Python 3.10+ / 建议使用 Python 3.10 以上版本：
```bash
# Clone the repository / 克隆代码仓库
git clone https://github.com/Daniel-Leestar/exl-migration-factory.git
cd exl-migration-factory

# Initialize virtual environment / 创建虚拟环境
python -m venv .venv
source .venv/bin/activate       # On Windows: .venv\Scripts\activate

# Install dependencies / 安装全局锁定依赖
pip install --upgrade pip
pip install -r requirements.txt
```

### Step 2: Download Dataset & Ingest Bronze Layer / 下载数据集并摄取 Bronze 原始层
```bash
# Ingest raw French motor insurance CSVs into Delta Lake Bronze layer
# 将原始法国车险 CSV 数据摄取至 Delta Lake Bronze 原始层
python cloud/bronze_ingest.py
```

### Step 3: Run Automated Test Suite / 执行自动化测试与对账验证
```bash
# Run test suite with pytest / 使用 pytest 运行全套测试
pytest tests/ -v

# Or use Python built-in runner / 或使用 Python 内置测试器
python -m unittest discover -s tests
```

### Step 4: Launch Governance Dashboard / 启动 Streamlit 交互治理工作台
```bash
# Launch interactive web application / 启动前端交互看板
streamlit run ui/app.py
```

---

## 6. Verification Status Tiers / 验证状态三等级定义

- 🟢 **Verified (High Trust / 最高可信)**:
  - Processed entirely via deterministic compiler path (sqlglot -> AST -> IR -> PySpark).
  - 0 discrepancies in row-by-row reconciliation against SQL Server standard answers.
  - 全程走确定性编译器路径，逐行对账与标准答案完全一致（差异数为 0）。
- 🔵 **Tested (AI-Augmented / AI 增强验证)**:
  - Handled via LLM fallback translator for complex dialect or SAS procedures.
  - Validated with 100% reconciliation parity and bounded self-healing iterations.
  - 由 LLM 辅助翻译，必须 100% 通过双轨对账与反例缩减测试。
- 🟡/🔴 **Needs review (Honest Boundary / 诚实边界待审)**:
  - Complex stored procedures or scripts where reconciliation failed after 3 self-healing attempts.
  - Transparently marked to demonstrate responsible system boundaries.
  - 自愈 3 次后仍未通过或不支持的语法，诚实展示系统边界，绝不带病上线。

---

## 7. Migration Engine, Human Sign-off & Dashboard / 迁移引擎、人工签核与仪表板

Full engine documentation: [`docs/engine.md`](docs/engine.md). Needs a Java runtime (8, 11 or 17) for PySpark.
引擎完整文档见 [`docs/engine.md`](docs/engine.md)。PySpark 需要 Java 运行环境（8、11 或 17）。

```bash
# Re-run all 12 jobs against the committed legacy answers, without SQL Server or an LLM
# 使用已提交的旧系统答案重跑全部 12 个作业（无需 SQL Server 与 LLM）
python -m engine run-all --legacy golden --llm off

# Publish the status table, reconciliation reports and CI gate packages
# 发布状态表、对账报告与 CI 门禁作业包
python -m engine export

# A named reviewer signs off a Verified / Tested job; only then may it reach Gold
# 具名审阅人签核 Verified / Tested 作业，之后才允许进入 Gold 层
python -m engine approve J05 --reviewer "Your Name" --notes "read code and report"

# Read-only dashboard: status table, code side by side, lineage, reconciliation, analytics
# 只读仪表板：状态表、新旧代码对照、血缘、对账报告、分析
streamlit run ui/app.py
```

Current results / 当前结果: **7 Verified, 0 Tested, 5 Needs review** (see `ui/mock_data/status_store.json`).

## 8. AI Tools & Existing Code Declaration / AI 工具与既有代码声明

See [`docs/ai_tools_declaration.md`](docs/ai_tools_declaration.md).

