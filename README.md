# EXL Migration Factory (Challenge 03: Agentic Data Platform Modernisation)
# EXL 自动化迁移工厂 (选题目三：智能数据平台现代化)

> **EXL & UCD Data & AI Hackathon 2026**  
> **Challenge Track / 赛题:** Challenge 03: Agentic Data Platform Modernisation (Legacy ETL/SAS to Azure Lakehouse)  
> **Team / 战队:** DNA  
> **Team Members & Ownership / 团队成员与分工:**
> - **Tianyue Li / 李天悦 (DevOps & Lakehouse Lead)**: Cloud lakehouse architecture, Bronze ingestion, Silver data contracts & quarantine routing, Gold governance admission gates, CI/CD automated reconciliation pipeline, and FinOps/SLA alerts.
> - **Pin-Yun Kuo / 郭小妘 (Team Lead, Migration Engine & Dashboard)**: Migration engine (`sqlglot` parsing, typed IR, rule-based PySpark codegen, reconciliation, counterexample shrinking, local-LLM path), legacy answers recorded from SQL Server, human sign-off, and the Streamlit dashboard.

---

## 1. Executive Summary & Core Philosophy / 项目概述与核心哲学

Enterprises face massive hurdles modernizing legacy data estates: tens of thousands of brittle T-SQL stored procedures and SAS actuarial scripts locked in on-premise silos. Unchecked LLM code generation introduces hallucination risks, numerical drift, and compliance violations.

**EXL Migration Factory** is a working prototype for that kind of estate (the challenge brief describes about 1,200 ETL jobs and 400 SAS procedures), demonstrated on **13 representative jobs** and targeting a modern Azure Lakehouse (PySpark + Delta Lake) under a non-negotiable architectural doctrine:

> ### **[Team First Principle: AI Proposes, the Compiler Disposes]**
> Large Language Models (LLMs) never hold autonomous write access or production authority. Fixed rules translate what they can through a typed Intermediate Representation (IR); an LLM proposes PySpark only for what the rules cannot handle, and its code is statically checked before it runs. Every candidate PySpark script must pass automated, mathematical, record-for-record reconciliation against the legacy system's own output recorded from SQL Server. Jobs failing reconciliation are capped at 3 self-healing iterations before being safely degraded to `Needs review`. Code that fails reconciliation, or that no person has signed off, is not promoted to the Gold layer.
>
> ### **【团队研发第一铁律：AI Proposes, the Compiler Disposes】**
> 大语言模型（LLM）绝不拥有自主写库权或生产发布权。固定规则通过带类型的中间表示（IR）翻译其能处理的部分；规则无法处理的部分才由 LLM 提出 PySpark 代码，且代码在执行前先经过静态检查。所有候选 PySpark 脚本必须通过与从 SQL Server 录制的旧系统输出的逐行对账。未通过对账的作业最多允许 3 次自愈重试，逾期安全降级为 `Needs review`，未通过对账或未经人工签核的代码不会发布到 Gold 层。

---

## 2. Quickstart / 快速开始

Needs Python 3.10+ and a Java runtime (8, 11 or 17) for PySpark. Run from the project root.
需要 Python 3.10+ 与 Java 运行环境（8、11 或 17）。在项目根目录执行。

```bash
# 1. Install dependencies / 安装依赖
pip install -r requirements.txt

# 2. Migrate every job and publish the results. Results are not stored in the repository.
#    This mode needs no SQL Server and no AI model: it replays the committed legacy answers.
#    迁移全部作业并发布结果。结果不存入仓库；此模式无需 SQL Server 与 AI 模型，使用已提交的旧系统答案。
python -m engine run-all --legacy golden --llm off
python -m engine export

# 3. Run the automated tests (data contracts + reconciliation gate)
#    运行自动化测试（数据契约与对账门禁）
pytest tests/ -v

# 4. Open the dashboard / 打开仪表板
streamlit run ui/app.py

# Optional: Bronze -> Silver -> reconciliation gate -> Gold in one command
# 可选：一键执行 Bronze -> Silver -> 对账门禁 -> Gold
python run_e2e_factory.py
```

With a local model (`ollama pull qwen2.5-coder:3b`) and SQL Server LocalDB, drop `--legacy golden --llm off`
to let the AI path run as well. Settings live in `.env` (copy `.env.example`).
若本机有模型（`ollama pull qwen2.5-coder:3b`）与 SQL Server LocalDB，去掉 `--legacy golden --llm off` 即可同时运行 AI 路径。
设置放在 `.env`（由 `.env.example` 复制）。

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
│   │                            # [CN] Silver 层数据契约校验与 Quarantine 隔离分流引擎 
│   └── gold_deploy.py           # [EN] Production admission gate, automated deployment & SLA alert engine
│                                # [CN] Gold 生产层严格准入门禁、自动发布与 SLA 告警引擎
├── jobs/                        # [EN] 13 migration jobs (J01-J10, S01-S03) plus the CI fixture J00_mock
│                                # [CN] 13 个迁移作业 (legacy 代码、meta.yaml、录制的旧系统答案) 与 CI 样板 J00_mock
│   ├── J00_mock/                # [EN] Decoupled baseline verification mock job package
│   └── ...                      # [EN] J01-J09 (T-SQL), S01-S03 (SAS Actuarial)
├── engine/                      # [EN] Migration compiler kernel (AST, IR v0, Codegen, Reconciler, Shrinker)
│                                # [CN] 迁移工厂核心编译器 (AST 语法树解析、确定性生成、对账比对、反例缩减)
├── ui/                          # [EN] Streamlit dashboard (status, code, lineage, reconciliation, AI audit, sign-off)
│                                # [CN] Streamlit 仪表板 (状态、代码对照、血缘、对账报告、AI 审计、人工签核)
├── data/                        # [EN] Physical Lakehouse tiers (.gitignore protected, clean fallbacks)
│   ├── raw/                     # [EN] Raw benchmark files (freMTPL2freq.csv, freMTPL2sev.csv)
│   ├── bronze/                  # [EN] Bronze Delta Lake tables
│   ├── silver/                  # [EN] Silver Delta Lake tables
│   ├── quarantine/              # [EN] Dead-Letter Quarantine isolation
│   └── gold/                    # [EN] Production Gold Marts & SLA alert logs
├── docs/                        # [EN] Architecture diagram, Responsible AI note, engine guide
│   ├── architecture_diagram.svg # [EN] 1920x1080 High-resolution vector architecture diagram
│   ├── architecture_diagram.mmd # [EN] Mermaid source code for architecture topology
│   ├── responsible_ai_note.md   # [EN] One-page Responsible AI note
│   └── engine.md                # [EN] Migration engine, dashboard and settings
├── infra/
│   └── main.bicep               # [EN] Azure storage template for the layers (not deployed)
├── tests/                       # [EN] Automated test suites (Bronze, Silver, Reconciliation, Gold)
├── run_e2e_factory.py           # [EN] Single-command end-to-end orchestrator
├── requirements.txt             # [EN] Dependency list
└── README.md                    # [EN] Official submission documentation
```

---

## 4. Medallion Lakehouse & Quarantine Architecture / 奖牌湖仓与隔离区架构

The figures in this table (99.82%, 1,224 rows) were measured by the lakehouse owner on the full freMTPL2 files; CI runs on a small sample and does not reproduce them. The migrated jobs currently read the sample under `datasets/base/`, not the Silver tables: the two tracks meet at the Gold gate.
本表中的数字（99.82%、1,224 行）由湖仓负责人在完整 freMTPL2 数据上测得；CI 使用小样本，不会重现这些数字。迁移后的作业目前读取 `datasets/base/` 下的样本，而不是 Silver 表：两条线在 Gold 门禁处汇合。

| Tier / 分层 | Storage & Format / 存储格式 | Governance & Contract Rules / 治理规则与数据契约 | Owner / 负责人 |
| :--- | :--- | :--- | :--- |
| **Bronze (原始层)** | Delta Lake / Parquet<br>`data/bronze/` | Preserves raw schema. Strictly appends `_ingested_at` and `_source_file` audit columns. Enforces append-only.<br>保持原始结构，追加系统审计字段，只追加不修改。 | Tianyue Li / 李天悦 |
| **Silver (清洗层)** | Delta Lake / Parquet<br>`data/silver/` | Enforces Schema casting, string trimming/uppercasing, defaults (`UNKNOWN`). Routes invalid records to Quarantine.<br>强制类型转换、分类文本大写与去空格、空值填充。合规率 99.82%。 | Tianyue Li / 李天悦 |
| **Quarantine (隔离区)** | Delta Lake / Parquet<br>`data/quarantine/` | Dead-Letter Queue (DLQ). Traps 1,224 rows violating `Exposure <= 1.0` or underage driving with `_quarantine_reasons`.<br>死信隔离区，精准拦截 1,224 条异常记录并打标违规成因，零污染下游。 | Tianyue Li / 李天悦 |
| **Gold (业务层)** | Delta Lake / Parquet<br>`data/gold/` | Production marts. ONLY admitted if `status in ["Verified", "Tested"]` AND `approved=True`. Protected by SLA alerts.<br>精算宽表与 KPI 聚合。仅允许双轨对账通过且经人工签核的作业入库。 | Tianyue Li / 李天悦 |

---

## 5. Data and Tools / 数据与工具

### Datasets / 数据集
- **French Motor Third-Party Liability Dataset (`freMTPL2freq`, `freMTPL2sev`)**:
  - Source: public actuarial dataset from the R package `CASdatasets`, also published on OpenML.
  - Licence: as stated by the dataset's publisher; check it before any reuse. This repository holds only a small sample (`datasets/base/`).
  - Scope: 678,013 risk records and 26,639 claim records.
- **Adversarial test rows**: made up by `engine/recon/advdata.py` (NULLs, repeated keys, mixed case, boundary values).
- **Zero Real Client / Personal Data**: All demonstrations and benchmarking use public or synthetic data only.

### Tools / 工具
- **`sqlglot` (MIT)**: Deterministic T-SQL dialect parsing, semantic AST tree extraction, and column-level lineage graph generation.
- **`PySpark` & `Delta Lake` (Apache-2.0)**: Distributed compute engine and ACID transaction lakehouse table format.
- **`Streamlit` (Apache-2.0)**: Interactive web control plane for lineage exploration, reconciliation proof inspection, and human sign-off.
- **Local LLM (`qwen2.5-coder:3b` via Ollama)**: Used strictly as a fallback code proposal agent on code syntax, schema structures, and synthetic counterexample diffs. It runs on the developer's machine; **no prompt is sent to an external API, and no customer or PII data is involved.**

---

## 6. Migration Engine, Human Sign-off & Dashboard / 迁移引擎、人工签核与仪表板

Full documentation: [`docs/engine.md`](docs/engine.md). 完整文档见 [`docs/engine.md`](docs/engine.md)。

- **Results are generated, not committed.** `python -m engine run-all` then `python -m engine export` write the status
  table, reconciliation reports, migrated code and sign-offs to folders that Git ignores. CI regenerates them on every run.
  **结果是生成的，不提交。** 状态表、对账报告、迁移后的代码与签核记录都写入 Git 忽略的目录；CI 每次运行都会重新生成。
- **Dashboard**: four pages. *Overview* (status, progress to Gold, Gold deployment gate), *Job detail* (code side by side,
  lineage, reconciliation, AI audit trail, sign-off and re-run), *Analytics*, and *Try your own code* (paste T-SQL to see
  whether the fixed rules can translate it).
  **仪表板**共四页：总览、作业详情（代码对照、血缘、对账、AI 审计、签核与重跑）、分析、试用自己的代码。
- **Human sign-off**: only Verified / Tested jobs can be signed off, in the dashboard or with
  `python -m engine approve <job> --reviewer "<name>"`. The Gold gate promotes nothing without it. There is no login in
  this prototype: the reviewer name is recorded as typed.
  **人工签核**：只有 Verified / Tested 作业可以签核；没有签核，Gold 门禁不放行。原型没有登录，审阅人姓名按输入记录。
- **Settings** (`.env`): `MF_OLLAMA_HOST`, `MF_OLLAMA_MODEL`, `MF_SQLSERVER_CONN`, `MF_LAKEHOUSE_ROOT`.
- **Cloud**: everything runs locally in Delta Lake format. `infra/main.bicep` is an Azure storage template that has
  **not** been deployed or validated; the `--cloud` options of the `cloud/` scripts are likewise untested by us.
  **云端**：目前全部在本机以 Delta Lake 格式运行。`infra/main.bicep` 是**未经部署验证**的 Azure 存储模板。

Latest results on the team's machine, with the local model on / 团队机器上的最新结果（启用本地模型）:
**13 jobs: 7 Verified, 1 Tested, 5 Needs review.** With `--llm off` the Tested job (J10) becomes Needs review.
