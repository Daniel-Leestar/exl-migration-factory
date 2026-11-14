# Error log / 錯誤紀錄

Each entry: symptom, root cause, fix, how to prevent it.
每筆記錄:症狀、根因、解法、如何預防。

---

## 2026-11-14 — CI reconciliation gate failed after the runner image changed / CI 對帳門禁在 Runner 映像換版後失敗

**Symptom / 症狀**
- PR 5's CI failed in `tests/test_reconciliation_gate.py` with
  `RuntimeError: job J01 needs a SparkSession; there is no non-Spark fallback`.
- The two Spark tests in `tests/test_silver_contracts.py` were reported as SKIPPED, and the gate step finished in 2.5 s.
- The commit under test only added dashboard files; the same code had passed the day before.
- PR 5 的 CI 在對帳門禁失敗,錯誤是 J01 拿不到 SparkSession;silver 的兩個 Spark 測試被跳過,門禁只跑了 2.5 秒。
  被測的 commit 只新增儀表板檔案,同樣的程式前一天是通過的。

**Root cause / 根因**
- `runs-on: ubuntu-latest` moved from the `ubuntu-24.04` image to `ubuntu-26.04` between the two runs.
  Installed Python packages were identical in both runs (pyspark 3.5.9, pandas 2.3.3, pyarrow 25.0.1).
- Spark could not start on the new image. PySpark 3.5 supports Java 8 / 11 / 17 only, and the workflow relied on
  whatever Java the image shipped. This is inferred, not observed directly: see the next point.
- The gate's Spark probe catches every exception and silently falls back to `spark = None`, so the real start-up
  error never reached the log and the failure surfaced later with a misleading message.
- `ubuntu-latest` 在兩次執行之間從 24.04 換成 26.04,兩次裝到的 Python 套件完全相同。
  新映像上 Spark 起不來:PySpark 3.5 只支援 Java 8 / 11 / 17,而 workflow 用的是映像內建的 Java。
  這是推論而非直接觀察,因為門禁的 Spark 探測把例外全部吞掉並退回 `spark = None`,真正的啟動錯誤沒有進 log。

**Fix / 解法**
- Added an `actions/setup-java@v4` step pinning Temurin 17 in `.github/workflows/ci.yml` (commit `8591990`).
- After the fix: silver tests 4 passed with none skipped, the gate ran with Spark (13 s), and the engine and
  dashboard tests ran in CI for the first time (121 passed, 1 skipped).
- 在 `ci.yml` 加入固定 Temurin 17 的步驟。修正後 silver 測試 4 個全跑、門禁有用 Spark 執行、
  引擎與儀表板測試首次在 CI 執行(121 通過、1 skip)。

**Prevention / 如何預防**
- Pin every runtime the build depends on (Python, Java), not only the Python packages. A floating image tag
  changes underneath an unchanged repository.
- When a red build follows a change that cannot plausibly cause it, compare the environment of the last green
  run first (image version, tool versions) before debugging the code.
- Open issue, not fixed here: the gate's probe should log the exception it catches, and CI should fail loudly
  when Spark is unavailable instead of running in a degraded mode. That test belongs to the CI owner.
- 建置依賴的所有執行環境(Python、Java)都要固定版本,不只是 Python 套件;浮動的映像標籤會在 repo 沒變的情況下改變。
- 當紅燈出現在不可能造成它的改動之後,先比對上一次綠燈的環境(映像版本、工具版本),再去查程式。
- 待處理(此處未修):門禁的探測應記錄它捕捉到的例外,且 Spark 不可用時 CI 應直接失敗,而不是降級執行。該測試屬於 CI 負責人。
