# EXL Migration Factory (Challenge 03: Agentic Data Platform Modernisation)
# EXL 自动化迁移工厂 (选题目三：智能数据平台现代化)

> **EXL & UCD Data & AI Hackathon 2026**  
> **Challenge Track / 赛题:** Challenge 03: Agentic Data Platform Modernisation (Legacy ETL/SAS to Azure Lakehouse)  
> **Team / 战队:** UCD Team (Challenge 03)  
> **Team Members & Ownership / 团队成员与分工:**
> - **Tianyue Li / 李天悦 (DevOps & Lakehouse Lead)**: Cloud lakehouse architecture, Bronze ingestion, Silver data contracts & quarantine routing, Gold governance admission gates, CI/CD automated reconciliation pipeline, and FinOps/SLA alerts.
> - **Pin-Yun Kuo / 郭小妘 (Compiler Lead)**: Deterministic migration engine, `sqlglot` AST parsing, IR Schema v0, deterministic PySpark codegen, row-by-row reconciliation engine, and minimal failure shrinker.
> - **Neeraj (Benchmarking & UI Lead)**: SQL Server 2022 Docker baseline execution, ground-truth `.parquet` export, and Streamlit interactive control plane (lineage graph, diff reports, and 1,200-job extrapolation).

---

## 1. Executive Summary & Core Philosophy / 项目概述与核心哲学

Enterprises face massive hurdles modernizing legacy data estates: tens of thousands of brittle T-SQL stored procedures and SAS actuarial scripts locked in on-premise silos. Unchecked LLM code generation introduces hallucination risks, numerical drift, and compliance violations.

**EXL Migration Factory** tackles this challenge across **1,200 ETL jobs and 400 SAS actuarial procedures** targeting a modern Azure Lakehouse (PySpark + Delta Lake) under a non-negotiable architectural doctrine:

> ### **[Team First Principle: AI Proposes, the Compiler Disposes]**
> Large Language Models (LLMs) never hold autonomous write access or production authority. LLMs propose candidate transformations within strictly bounded, Pydantic-validated Intermediate Representations (IR). Every candidate PySpark script must pass automated, mathematical, record-for-record reconciliation against authoritative SQL Server ground truth (`oracle_output.parquet`). Jobs failing reconciliation are capped at 3 self-healing iterations before being safely degraded to `Needs review`. Zero defective code enters the Gold production layer.
>
> ### **【团队研发第一铁律：AI Proposes, the Compiler Disposes】**
> 大语言模型（LLM）绝不拥有自主写库权或生产发布权。LLM 仅在严格受限且经过 Pydantic 校验的中间表示（IR）范围内提出代码建议。所有候选 PySpark 脚本必须通过与权威旧系统标准答案（`oracle_output.parquet`）的全量逐行数学对账。未通过对账的作业最多允许 3 次自愈重试，逾期安全降级为 `Needs review`，绝不允许任何带病代码注入 Gold 生产层。

---

## 2. 3-Step Quickstart Guide / 3 步快速开始与评委复现指南

Run these 3 commands in a clean environment (Python 3.10+):

```bash
# Step 1: Install pinned dependencies / 安装全局锁定依赖
pip install -r requirements.txt

# Step 2: Run complete automated test suite (Contracts + Reconciliation Gates)
# 执行全量自动化测试套件 (包含数据契约校验与双轨对账门禁)
pytest tests/ -v

# Step 3: Run end-to-end Medallion & Governance Pipeline
# 一键运行端到端 Medallion 湖仓摄取、契约清洗、对账门禁与 Gold 发布流水线
python run_e2e_factory.py
```

### Launch Interactive Governance Workbench / 启动 Streamlit 交互治理大屏
```bash
streamlit run ui/app.py
```

---

## 3. Repository Architecture Topology / 代码仓库拓扑结构

```plaintext
exl-migration-factory/
├── .github/
│   └── workflows/
│       └── ci.yml               # [EN] GitHub Actions CI with nightly cron & reconciliation blocking gate
│                                # [CN] GitHub Actions CI 流水线 (包含每日定时回归与强阻断对账门禁)
├── cloud/                       # [EN] Azure Lakehouse storage, contracts & deployment tier
│                                # [CN] Azure 湖仓存储、数据契约与部署调度层
│   ├── bronze_ingest.py         # [EN] Raw CSV -> Delta Lake (Append-only Bronze with audit metadata)
│   │                            # [CN] 原始数据到 Delta Lake Bronze 层摄取管道 (只追加与元数据注入)
│   ├── silver_contracts.py      # [EN] Schema casting, domain validation & Quarantine isolation routing
│   │                            # [CN] Silver 层数据契约校验与 Quarantine 隔离分流引擎 (拦截 1,224 异常)
│   └── gold_deploy.py           # [EN] Production admission gate, automated deployment & SLA alert engine
│                                # [CN] Gold 生产层严格准入门禁、自动发布与 SLA 告警引擎
├── jobs/                        # [EN] 12 migration job packages (J00_mock, J01-J09, S01-S03)
│                                # [CN] 12 个迁移作业自包含包 (包含 legacy.sql, meta.yaml, oracle_output.parquet)
│   ├── J00_mock/                # [EN] Decoupled baseline verification mock job package
│   └── ...                      # [EN] J01-J09 (T-SQL), S01-S03 (SAS Actuarial)
├── engine/                      # [EN] Migration compiler kernel (AST, IR v0, Codegen, Reconciler, Shrinker)
│                                # [CN] 迁移工厂核心编译器 (AST 语法树解析、确定性生成、对账比对、反例缩减)
├── ui/                          # [EN] Streamlit executive workbench (lineage graphs, diff inspector, ROI)
│                                # [CN] Streamlit 交互治理工作台 (字段级血缘、对账报告、1,200 作业商业推估)
├── data/                        # [EN] Physical Lakehouse tiers (.gitignore protected, clean fallbacks)
│   ├── raw/                     # [EN] Raw benchmark files (freMTPL2freq.csv, freMTPL2sev.csv)
│   ├── bronze/                  # [EN] Bronze Delta Lake tables
│   ├── silver/                  # [EN] Silver Delta Lake tables (99.82% pass rate)
│   ├── quarantine/              # [EN] Dead-Letter Quarantine isolation (1,224 anomalous records)
│   └── gold/                    # [EN] Production Gold Marts & SLA alert logs
├── docs/                        # [EN] Hackathon submission deliverables (Diagrams, Demo script, Deck)
│   ├── architecture_diagram.svg # [EN] 1920x1080 High-resolution vector architecture diagram
│   ├── architecture_diagram.mmd # [EN] Mermaid source code for architecture topology
│   ├── responsible_ai_note.md   # [EN] Single-page Responsible AI declaration
│   ├── demo_script_3min.md      # [EN] Timestamped 3-minute video recording storyboard
│   └── deck_architecture_ops.md # [EN] Slide-by-slide copy for solution deck
├── tests/                       # [EN] Automated test suites (Bronze, Silver, Reconciliation, Gold)
├── run_e2e_factory.py           # [EN] Single-command end-to-end orchestrator
├── requirements.txt             # [EN] Pinned global dependency manifest
└── README.md                    # [EN] Official submission documentation
```

---

## 4. Medallion Lakehouse & Quarantine Architecture / 奖牌湖仓与隔离区架构

| Tier / 分层 | Storage & Format / 存储格式 | Governance & Contract Rules / 治理规则与数据契约 | Owner / 负责人 |
| :--- | :--- | :--- | :--- |
| **Bronze (原始层)** | Delta Lake / Parquet<br>`data/bronze/` | Preserves raw schema. Strictly appends `_ingested_at` and `_source_file` audit columns. Enforces append-only.<br>保持原始结构，追加系统审计字段，只追加不修改。 | Tianyue Li / 李天悦 |
| **Silver (清洗层)** | Delta Lake / Parquet<br>`data/silver/` | Enforces Schema casting, string trimming/uppercasing, defaults (`UNKNOWN`). Routes invalid records to Quarantine.<br>强制类型转换、分类文本大写与去空格、空值填充。合规率 99.82%。 | Tianyue Li / 李天悦 |
| **Quarantine (隔离区)** | Delta Lake / Parquet<br>`data/quarantine/` | Dead-Letter Queue (DLQ). Traps 1,224 rows violating `Exposure <= 1.0` or underage driving with `_quarantine_reasons`.<br>死信隔离区，精准拦截 1,224 条异常记录并打标违规成因，零污染下游。 | Tianyue Li / 李天悦 |
| **Gold (业务层)** | Delta Lake / Parquet<br>`data/gold/` | Production marts. ONLY admitted if `status in ["Verified", "Tested"]` AND `approved=True`. Protected by SLA alerts.<br>精算宽表与 KPI 聚合。仅允许双轨对账通过且经人工签核的作业入库。 | Tianyue Li / 李天悦 |

---

## 5. Mandatory Hackathon Disclosures / 官方参赛强制披露声明

### Datasets Used / 数据集使用声明
- **French Motor Third-Party Liability Dataset (`freMTPL2freq`, `freMTPL2sev`)**:
  - Source: Public actuarial research dataset hosted on Hugging Face / OpenML.
  - Licensing: Open Access (GPL-2.0 / CC0).
  - Scope: 678,013 risk records and 26,639 claim records.
- **Synthetic SCD2 Stream**: Generated via `Faker` (MIT License) for simulating policy renewals and rating tier transitions.
- **Zero Real Client / Personal Data**: All demonstrations and benchmarking use 100% anonymized or synthetic data.

### AI & Open-Source Tools Declared / 声明采用的 AI 与开源工具
- **`sqlglot` (MIT)**: Deterministic T-SQL dialect parsing, semantic AST tree extraction, and column-level lineage graph generation.
- **`PySpark` & `Delta Lake` (Apache-2.0)**: Distributed compute engine and ACID transaction lakehouse table format.
- **`Streamlit` (Apache-2.0)**: Interactive web control plane for lineage exploration, reconciliation proof inspection, and human sign-off.
- **Local LLM (`qwen2.5-coder:3b` via Ollama)**: Used strictly as a fallback code proposal agent on code syntax, schema structures, and synthetic counterexample diffs. It runs on the developer's machine; **no prompt is sent to an external API, and no customer or PII data is involved.**
- **AI coding assistants used during development**: see [`docs/ai_tools_declaration.md`](docs/ai_tools_declaration.md).

---

## 6. Migration Engine, Human Sign-off & Dashboard / 迁移引擎、人工签核与仪表板

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

## 7. AI Tools & Existing Code Declaration / AI 工具与既有代码声明

See [`docs/ai_tools_declaration.md`](docs/ai_tools_declaration.md).
