# 核心引擎設計(Pin-Yun 負責範圍)

日期:2026-11-12 · 對應總體 spec:EXL Hackathon 選題三 設計規格(2026-11-08)

## 範圍

解析與 IR、程式產生器、LLM 翻譯器、對帳引擎、等級判定與輸出 JSON。
UI、分析頁、雲端、CI 不在此文件範圍。

## 已定案的決策

| 決策 | 選擇 | 沒選的方案與原因 |
|---|---|---|
| 規則路徑的產出 | 一支 PySpark 程式,固定長相 `run(spark, tables)` | sqlglot 直接轉 Spark SQL:最快,但陷阱(整數除法等)不會被處理 |
| 產生的程式怎麼跑 | 同一個 Python 行程內執行,先做 import/屬性白名單檢查 | 每次開新行程:隔離較好,但每次重啟 Spark 太慢,縮減反例跑不動 |
| 比對用什麼做 | 純 Python(字典 + Decimal) | pandas:有 NULL 的整數欄會被偷偷轉成小數 |
| 舊系統答案 | 可替換插槽:答案檔 / SQL Server | 只用答案檔:無法對新輸入重跑,做不了縮減 |
| 舊系統實體 | 本機已安裝的 SQL Server LocalDB | Docker 映像檔:要下載 1.6 GB,且 8 GB 記憶體很擠 |
| LLM | 可替換插槽:Ollama `qwen2.5-coder:3b` + 測試用假替身 | 寫死單一供應商 |

## 實作中才發現、因而改變設計的三件事

1. **這台 Windows 上 Spark 的 Python 工作行程一啟動就當掉**(`Python worker exited unexpectedly`,
   換 Java 8/21、換直譯器路徑都一樣,根因未查明)。
   影響:`spark.createDataFrame` 與任何 Python UDF 都不能用。
   對策:輸入表改寫成 JSON-lines 暫存檔由 JVM 讀取;小數截斷除法改用純 Spark 運算式。
   副作用是產生的程式不依賴任何自訂函式庫,部署更單純。
2. **SQL Server 的小數除法是截斷,不是四捨五入**(`2.0/3 = 0.666666`),`AVG(decimal)` 也是。
   Spark 是四捨五入。對策:先截斷 Spark 的商,若 Spark 自己的進位已經多進一格再退回一格,
   判斷方式是「截斷後的商 × 除數」的絕對值是否超過被除數。
   需要 `spark.sql.decimalOperations.allowPrecisionLoss=false`,產生的程式會自行設定。
3. **整數常數在小數運算中依位數定型**:`decimal(18,2) / 3` 的結果是 `decimal(22,6)`,
   但除以 int 欄位是 `decimal(29,13)`。型別推斷已照此實作。

以上 2、3 的預期值都在真實 SQL Server 2016 SP1 上實測過,並寫進 `tests/test_codegen.py`。

## 模組

| 檔案 | 職責 |
|---|---|
| `engine/model.py` | 共用的「表」形狀、帶型別表頭的 CSV 讀寫、輸入指紋 |
| `engine/job.py` | 讀 job 資料夾或單一 `.sql`/`.sas` 檔 |
| `engine/ir.py` | IR 節點、JSON 互轉、`OutOfScope` |
| `engine/parser.py` | T-SQL → IR;不認得的一律拒絕並說明原因 |
| `engine/ir_types.py` | 欄位名稱解析、依 SQL Server 規則推斷型別 |
| `engine/codegen.py` | IR → PySpark;所有語意差異規則集中於此,並記錄套用了哪些 |
| `engine/lineage.py` | 由 IR 推導欄位 lineage;LLM 路徑只做文字比對並標示未驗證 |
| `engine/sandbox.py` | 檢查並執行產生的程式 |
| `engine/llm/` | LLM 連線插槽、紀錄、翻譯與修正的 prompt、真實資料防護 |
| `engine/legacy/runner.py` | 舊系統插槽:答案檔 / SQL Server |
| `engine/recon/` | 比對器、刁鑽資料、縮減器 |
| `engine/pipeline.py` | 串接全部並判定等級;絕不丟出例外 |
| `engine/__main__.py` | 命令列 |

## 等級判定

- Verified:規則路徑 + 所有有答案的資料集一致,且其中包含刁鑽資料
- Tested:LLM 路徑 + 同上條件
- Needs review:其餘。規則路徑不一致時**不會**改交 LLM 補救,因為那代表規則有錯,應由人來看

## 刻意不做

完整 T-SQL 支援、把 LLM 程式關進獨立行程、Claude/GPT API 接頭、效能調校、SAS 實際執行。
