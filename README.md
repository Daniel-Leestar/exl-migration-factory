# EXL Migration Factory (Challenge 03: Agentic Data Platform Modernisation)

> **EXL & UCD Data & AI Hackathon 2026**  
> **赛题：** Legacy ETL/SAS to Azure Lakehouse  
> **团队：** UCD Team (Challenge 03)  
> **成员分工：**
> - **李天悦 (Tianyue)**: 湖仓架构设计、CI/CD 自动化流水线、Bronze 摄取与 Gold 自动化部署
> - **郭小妘 (Pin-Yun)**: 编译内核开发（AST 解析、IR Schema、确定性代码生成、对账引擎、反例缩减）
> - **Neeraj**: 遗留数据基准导出、SQL Server 环境运维、Streamlit 治理与交互工作台

---

## 核心设计理念 (Philosophy)

> **【团队研发第一铁律：AI Proposes, the Compiler Disposes】**  
> 大语言模型 (LLM) 永远不拥有代码与迁移的最终决定权。LLM 产出要么是经过语法检查并符合 Schema 约束的结构化 IR，要么是必须通过严格对账测试的 PySpark 脚本。未能通过自动化对账的作业最多允许自愈修正 3 次，逾期直接降级为 Needs review，绝不允许任何带病代码注入 Gold 生产层。

---

## 项目目录拓扑 (Directory Structure)

```plaintext
exl-migration-factory/
├── .github/
│   └── workflows/
│       └── ci.yml               # [Tianyue] GitHub Actions CI 流水线 (自动化对账触发)
├── cloud/                       # [Tianyue] 湖仓架构与部署
│   ├── bronze_ingest.py         # CSV -> Delta Lake (Bronze 原始层摄取)
│   ├── silver_contracts.py      # Data Contracts 验证与清洗层
│   └── gold_deploy.py           # 验证通过 Job 的自动化部署与调度
├── jobs/                        # [Neeraj / Pin-Yun] 12 个迁移作业集合
│   ├── J01/                     # 作业独立包 (legacy.sql, meta.yaml, oracle_output.parquet)
│   ├── ...                      # J02 至 J09 (T-SQL)
│   └── S01/ ... S03/            # S01 至 S03 (SAS)
├── engine/                      # [Pin-Yun] 迁移工厂核心引擎
│   ├── parser.py                # sqlglot T-SQL AST 解析器
│   ├── ir_schema.py             # IR 结构定义与 Pydantic 校验模型
│   ├── codegen.py               # 确定性规则代码生成器 (IR -> PySpark)
│   ├── llm_fallback.py          # LLM 兜底翻译器与 Prompt 审计记录器
│   ├── reconciler.py            # 逐行对账引擎 (Diff、Count、Sum 校验)
│   └── shrinker.py              # 失败测试用例自动缩减器 (最小重现反例)
├── ui/                          # [Neeraj] Streamlit 交互与治理工作台
│   ├── app.py                   # Streamlit 主入口
│   ├── mock_data/               # 前期解耦使用的假 JSON 存根
│   └── pages/                   # 总览看板、Job 详情、Lineage 族谱、对账报告、商业分析
├── data/                        # 基础数据目录 (Git 忽略，仅存样例或生成脚本)
│   ├── raw/                     # 原始数据
│   ├── bronze/                  # Delta Lake Bronze 层
│   ├── silver/                  # Delta Lake Silver 层
│   └── gold/                    # Delta Lake Gold 层
├── docs/                        # [Tianyue / All] 参赛终验交付物
│   ├── architecture_diagram.png # 单页系统架构图
│   └── responsible_ai_note.pdf  # 单页 RAI 声明文档
├── tests/                       # 单元与集成测试用例
├── requirements.txt             # 全局依赖锁定
└── README.md                    # 评委部署与一键复现指南
```

---

## 快速开始 (Quickstart)

### 1. 环境准备与依赖安装
建议使用 Python 3.10+：
```bash
python -m venv .venv
source .venv/bin/activate  # Windows: .venv\Scripts\activate
pip install --upgrade pip
pip install -r requirements.txt
```

### 2. 执行自动化测试
```bash
pytest tests/ -v
```

### 3. 启动 Streamlit 交互看板
```bash
streamlit run ui/app.py
```

