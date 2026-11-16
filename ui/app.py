"""Migration Factory dashboard (Streamlit). Run from the project root: `streamlit run ui/app.py`.
Migration Factory 儀表板(Streamlit)。在專案根目錄執行:`streamlit run ui/app.py`。

It shows what `python -m engine export` published. A person can sign a job off, withdraw a sign-off,
re-run a job, start the Gold deployment, or check how a new piece of legacy code would be handled.
The page itself never decides a status: the engine and the reconciliation do.
顯示 `python -m engine export` 發布的結果。使用者可以簽核、撤回簽核、重跑 job、啟動 Gold 部署,
或查看一段新的舊程式會如何被處理。頁面本身不決定任何狀態:狀態由引擎與對帳決定。
"""
from __future__ import annotations

import html
import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# `streamlit run ui/app.py` puts ui/ on the path, not the project root.
# `streamlit run ui/app.py` 只會把 ui/ 放進路徑,不含專案根目錄。
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ui.actions import (ActionError, deploy_gold, model_status, rerun, settings, sign_off, triage,  # noqa: E402
                        withdraw)
from ui.data import (LEVELS, ROOT, UiDataError, blocker_type, breakdown, funnel, gold_ready, job_rows,  # noqa: E402
                     lineage_dot, load_ai_log, load_job_files, load_lineage, load_report, load_store, next_step,
                     report_summary)

UI_DIR = ROOT / "ui" / "mock_data"
PAGES = ["Overview", "Job detail", "Analytics", "Try your own code"]
LEVEL_HELP = {
    "Verified": "Translated by fixed rules; output matched legacy on the sample, adversarial and empty datasets.",
    "Tested": "Translated by the AI model; output matched legacy on every dataset. Needs a careful human read.",
    "Needs review": "Did not match legacy, or there is no legacy output to compare against.",
}
LEVEL_CLASS = {"Verified": "ok", "Tested": "warn", "Needs review": "bad"}
# Columns shown by default on the overview; the rest sit behind a toggle and on the job page.
# 總覽預設顯示的欄位;其餘欄位收在開關後面,並可在 job 頁面看到。
MAIN_COLUMNS = ["Job", "Name", "Category", "Status", "Human approved", "Reason"]
COLUMN_HELP = {
    "Job": "The job's id.",
    "Name": "What the job computes.",
    "Category": "Kind of job: aggregation, cleansing, join and so on.",
    "Dialect": "Legacy language: T-SQL or SAS.",
    "Path": "IR = translated by fixed rules. LLM = needed the AI model.",
    "Status": "Verified, Tested or Needs review. See the guide at the bottom of the page.",
    "Reconciled": "Whether the new output matched the legacy output.",
    "Discrepancies": "Number of rows that did not match.",
    "LLM iterations": "How many times the AI model was asked: first try plus repairs.",
    "Human approved": "Whether a named person signed the job off. Required before Gold.",
    "Reviewer": "Who signed the job off.",
    "Reason": "Why a job still needs review. Empty for passing jobs.",
}

# The header tooltip appears on hover and fades out by itself after five seconds.
# 表頭提示在滑鼠移上時出現,五秒後自動淡出。
STYLE = """
<style>
[data-testid="stSidebar"], [data-testid="stSidebarCollapsedControl"] { display: none; }
.block-container { padding-top: 2.2rem; max-width: 1280px; }
.mf-head { display: flex; align-items: baseline; justify-content: space-between; gap: 1rem;
           border-bottom: 3px solid #1f6f5c; padding-bottom: .5rem; margin-bottom: .9rem; }
.mf-head h1 { font-size: 1.7rem; margin: 0; padding: 0; color: #12312a; }
.mf-head .sub { color: #5b6b66; font-size: .9rem; }
.mf-head .when { color: #5b6b66; font-size: .8rem; white-space: nowrap; }
[data-testid="stMetric"] { background: #ffffff; border: 1px solid #e3e8e5; border-radius: 10px; padding: .7rem .9rem; }
.mf-wrap { overflow-x: auto; border: 1px solid #e3e8e5; border-radius: 10px; background: #ffffff; }
table.mf { border-collapse: collapse; width: 100%; font-size: .88rem; }
table.mf th { position: relative; text-align: left; background: #f1f5f3; color: #12312a; padding: .55rem .7rem;
              border-bottom: 1px solid #dbe3df; white-space: nowrap; cursor: help; }
table.mf td { padding: .5rem .7rem; border-bottom: 1px solid #eef2f0; vertical-align: top; color: #1c2a26; }
table.mf tr:last-child td { border-bottom: none; }
table.mf tr:hover td { background: #f8fbf9; }
table.mf td.reason { color: #5b6b66; max-width: 30rem; }
table.mf th .tip { visibility: hidden; opacity: 0; position: absolute; top: calc(100% + 2px); left: .4rem; z-index: 20;
                   width: max-content; max-width: 15rem; white-space: normal; background: #12312a; color: #ffffff;
                   font-weight: 400; font-size: .76rem; line-height: 1.3; padding: .35rem .55rem; border-radius: 6px;
                   pointer-events: none; }
table.mf th:hover .tip { animation: mf-tip 5s forwards; }
@keyframes mf-tip { 0% { visibility: visible; opacity: 0; } 4% { visibility: visible; opacity: 1; }
                    92% { visibility: visible; opacity: 1; } 100% { visibility: hidden; opacity: 0; } }
.pill { display: inline-block; padding: .1rem .55rem; border-radius: 999px; font-size: .78rem; font-weight: 600;
        white-space: nowrap; }
.pill.ok { background: #d9f2e3; color: #14532d; } .pill.warn { background: #fdf0c8; color: #7a5200; }
.pill.bad { background: #fbdcdc; color: #8a1c1c; } .pill.none { background: #eceff1; color: #45525a; }
.mf-next { background: #eef6f3; border-left: 4px solid #1f6f5c; border-radius: 6px; padding: .6rem .9rem;
           margin: .4rem 0 .8rem 0; color: #12312a; }
</style>
"""


def pill(text: str, kind: str) -> str:
    return f'<span class="pill {kind}">{html.escape(str(text))}</span>'


def table_html(rows: list[dict], columns: list[str]) -> str:
    """The overview table as HTML, so headers can carry a hover explanation and statuses a coloured pill.
    把總覽表畫成 HTML,讓表頭能帶滑鼠提示、狀態能顯示成彩色標籤。

    Every value is escaped: job names and reasons come from files and must not be read as markup.
    所有值都經過跳脫:job 名稱與原因來自檔案,不可被當成標記解讀。
    """
    head = "".join(f'<th>{html.escape(c)}<span class="tip">{html.escape(COLUMN_HELP.get(c, ""))}</span></th>'
                   for c in columns)
    body = []
    for row in rows:
        cells = []
        for column in columns:
            value = row[column]
            if column == "Status":
                cell = pill(value, LEVEL_CLASS.get(value, "none"))
            elif column in ("Human approved", "Reconciled"):
                cell = pill("Yes", "ok") if value else pill("No", "none")
            else:
                cell = html.escape("" if value is None else str(value))
            cells.append(f'<td class="{"reason" if column == "Reason" else ""}">{cell}</td>')
        body.append("<tr>" + "".join(cells) + "</tr>")
    return f'<div class="mf-wrap"><table class="mf"><thead><tr>{head}</tr></thead><tbody>{"".join(body)}</tbody></table></div>'


def finish(action, message: str) -> None:
    """Run an action. On success reload the page data and show the message; a refusal is shown in place,
    so the person stays on the form they were filling in.
    執行一個動作。成功時重新載入頁面資料並顯示訊息;被拒絕時就地顯示原因,讓使用者留在原本填寫的表單。
    """
    try:
        action()
    except ActionError as error:
        st.error(str(error))
        return
    st.session_state["flash"] = ("success", message)
    st.rerun()


@st.cache_data(ttl=15, show_spinner=False)
def cached_model_status(host: str, model: str) -> tuple[bool, str]:
    # Cached briefly so an unreachable server does not slow down every click.
    # 短暫快取,避免連不上伺服器時每次點擊都變慢。
    return model_status(host, model)


# ---------- Overview / 總覽 ----------

def overview(store: dict) -> None:
    metrics, stages = store["metrics"], funnel(store)
    cells = st.columns(5)
    cells[0].metric("Jobs", metrics["total_jobs"])
    cells[1].metric("Verified", metrics["verified"], help=LEVEL_HELP["Verified"])
    cells[2].metric("Tested", metrics["tested"], help=LEVEL_HELP["Tested"])
    cells[3].metric("Needs review", metrics["needs_review"], help=LEVEL_HELP["Needs review"])
    cells[4].metric("Signed off for Gold", stages["signed_off"], help="Reconciled AND signed off by a person.")

    st.markdown("##### Progress to the Gold layer")
    left, right = st.columns(2)
    total, reconciled = max(stages["jobs"], 1), stages["reconciled"]
    left.progress(reconciled / total, text=f'Reconciled against legacy: {reconciled} of {stages["jobs"]} jobs')
    right.progress(stages["signed_off"] / max(reconciled, 1),
                   text=f'Signed off by a person: {stages["signed_off"]} of {reconciled} reconciled jobs')

    st.markdown("##### Jobs")
    left, right = st.columns([3, 1])
    wanted = left.multiselect("Status", LEVELS, default=list(LEVELS), label_visibility="collapsed")
    every_column = right.toggle("Show all columns", value=False)
    rows = [row for row in job_rows(store) if row["Status"] in wanted]
    columns = list(rows[0]) if rows and every_column else MAIN_COLUMNS
    if rows:
        st.markdown(table_html(rows, columns), unsafe_allow_html=True)
        st.caption("Hover over a column name for a short explanation. Open a job on the Job detail page.")
    else:
        st.info("No job has the selected status.")

    st.markdown("##### Deploy to Gold")
    ready = gold_ready(store)
    if ready:
        st.write(f'{len(ready)} job(s) are reconciled and signed off: {", ".join(ready)}.')
    else:
        st.warning("No job has been signed off yet, so a deployment would promote nothing. A machine result never "
                   "counts as an approval: open a Verified or Tested job and use its Review & actions tab.")
    lake = settings(ROOT)["lakehouse_root"]
    st.caption(f"Writes to the Gold layer under `{lake}` (set MF_LAKEHOUSE_ROOT in .env to change it). "
               "Jobs that are not reconciled and signed off are refused by the gate.")
    if st.button("Run the Gold deployment gate", disabled=not ready):
        with st.spinner("Running the Gold deployment gate."):
            try:
                st.session_state["deploy_output"] = deploy_gold(ROOT)
            except ActionError as error:
                st.error(str(error))
                return
        st.session_state["flash"] = ("success", "Gold deployment finished. See its output on the Overview page.")
        st.rerun()
    if st.session_state.get("deploy_output"):
        with st.expander("Output of the last Gold deployment"):
            st.code(st.session_state["deploy_output"], language="text")

    with st.expander("What do the levels and columns mean?"):
        st.markdown("**Levels**")
        for level in LEVELS:
            st.markdown(f"- **{level}** - {LEVEL_HELP[level]}")
        st.markdown("**Columns**")
        for name, text in COLUMN_HELP.items():
            st.markdown(f"- **{name}** - {text}")


# ---------- Job detail / 單一 job ----------

def report_section(report: dict | None) -> None:
    verdict = report_summary(report)
    (st.success if report and report.get("overall_passed") else st.info)(verdict)
    if report is None:
        return
    if report.get("reason"):
        st.error(report["reason"])
    summary = report.get("summary") or {}
    if summary.get("legacy_rows") is not None:
        cells = st.columns(4)
        cells[0].metric("Legacy rows", summary["legacy_rows"])
        cells[1].metric("Spark rows", summary["spark_rows"], delta=summary["count_diff"] or None)
        cells[2].metric("Mismatched rows", summary["mismatched_rows"])
        cells[3].metric("Dataset shown", report.get("dataset") or "-")
    if report.get("datasets"):
        st.markdown("**Datasets**")
        st.dataframe(pd.DataFrame(report["datasets"]), hide_index=True, width="stretch")
    if report.get("field_checks"):
        st.markdown("**Column totals (legacy vs Spark)**")
        st.dataframe(pd.DataFrame(report["field_checks"]), hide_index=True, width="stretch")
    if report.get("counterexamples"):
        st.markdown("**Counterexamples** (synthetic or sample rows; at most 3)")
        st.json(report["counterexamples"], expanded=True)
    if report.get("shrinking_metadata"):
        meta = report["shrinking_metadata"]
        st.markdown(f'**Smallest failing input**: shrunk from {meta.get("original_input_rows")} to '
                    f'{meta.get("shrunk_input_rows")} input rows in {meta.get("reruns")} re-runs.')
        st.json({k: meta.get(k) for k in ("inputs", "legacy_output", "spark_output")}, expanded=False)


def code_section(job: dict, files: dict) -> None:
    left, right = st.columns(2)
    left.markdown(f'**Legacy ({job.get("dialect", "")})**')
    left.code(files["legacy"], language="sql")  # no SAS highlighter; SQL colouring is close enough
    right.markdown("**PySpark**")
    if files["generated"] is None:
        right.info("No migrated code is available for this job. Re-run it on the Review & actions tab.")
    else:
        right.caption(files["generated_origin"])
        right.code(files["generated"], language="python")

    st.markdown("**Why the PySpark looks this way**")
    if job.get("rules_applied"):
        st.write("The fixed rules added code to reproduce these SQL Server behaviours, which plain PySpark would get wrong:")
        for rule in job["rules_applied"]:
            st.markdown(f"- {rule}")
    elif job.get("path") == "LLM":
        st.write("The fixed rules could not translate this job"
                 + (f' ({job["out_of_scope_reason"]})' if job.get("out_of_scope_reason") else "")
                 + ", so the AI model wrote the code. See the AI audit tab for exactly what it was asked.")
    else:
        st.write("No special legacy behaviour had to be reproduced for this job.")


def audit_section(job: dict) -> None:
    calls = load_ai_log(UI_DIR, job["job_id"])
    if not calls:
        if job.get("path") == "LLM":
            st.info("The AI model was not called for this job in the last run (it was switched off or unreachable).")
        else:
            st.success("No AI model was involved: this job was translated entirely by fixed rules.")
        return
    seconds = sum(call.get("seconds") or 0 for call in calls)
    cells = st.columns(3)
    cells[0].metric("Model calls", len(calls))
    cells[1].metric("Model", calls[0].get("model", "-"))
    cells[2].metric("Total model time", f"{seconds:.0f} s")
    st.caption("Every call is logged in full. Prompts contain the legacy code, table structures and made-up rows only; "
               "values from real data are withheld in code.")
    for number, call in enumerate(calls, start=1):
        label = f'{number}. {call.get("purpose", "call")} - {call.get("seconds", "?")} s - {call.get("time", "")}'
        with st.expander(label + ("  (failed)" if call.get("error") else "")):
            if call.get("error"):
                st.error(call["error"])
            st.markdown("**Prompt sent to the model**")
            st.code(call.get("prompt", ""), language="text")
            if call.get("response"):
                st.markdown("**Model answer**")
                st.code(call["response"], language="markdown")
            with st.popover("Standing instructions (system prompt)"):
                st.code(call.get("system", ""), language="text")


def actions_section(job: dict) -> None:
    job_id, approval = job["job_id"], job.get("human_approval") or {}

    st.markdown("#### Sign-off")
    if approval.get("approved"):
        st.success(f'Signed off by {approval.get("reviewer")} at {approval.get("timestamp")}.')
        if st.button("Withdraw sign-off", key=f"withdraw-{job_id}"):
            finish(lambda: withdraw(ROOT, job_id), f"Sign-off for {job_id} withdrawn.")
    elif job.get("status") in ("Verified", "Tested"):
        st.write("Read the Code and Reconciliation tabs first. Signing off states that you, a person, "
                 "accept this migrated code for the Gold layer.")
        with st.form(f"sign-{job_id}"):
            reviewer = st.text_input("Your name")
            notes = st.text_area("Notes (optional)", placeholder="What you checked, anything to watch")
            confirmed = st.checkbox("I have read the migrated code and the reconciliation report for this job.")
            if st.form_submit_button("Sign off for Gold", type="primary"):
                finish(lambda: sign_off(ROOT, job_id, reviewer, notes, confirmed), f"{job_id} signed off by {reviewer.strip()}.")
        st.caption("There is no login in this demo: the name is recorded as typed. "
                   "The sign-off lapses automatically if the code changes or the job stops passing.")
    else:
        st.info("Only Verified or Tested jobs can be signed off. This job still needs review: "
                + (job.get("reason") or "no reason recorded"))

    st.divider()
    st.markdown("#### Re-run this job")
    config = settings(ROOT)
    ready, message = cached_model_status(config["ollama_host"], config["ollama_model"])
    (st.success if ready else st.warning)(f"Local AI model: {message}")
    use_model = st.checkbox("Use the local AI model for parts the fixed rules cannot translate",
                            value=ready, disabled=not ready, key=f"model-{job_id}")
    st.caption("Runs the engine in a separate process, then republishes the results. With the AI model this can take "
               "several minutes. If the migrated code changes, an existing sign-off is cancelled.")
    if st.button("Re-run", key=f"rerun-{job_id}"):
        with st.spinner(f"Re-running {job_id}. Keep this page open."):
            try:
                st.session_state["rerun_output"] = (job_id, rerun(ROOT, job_id, use_model))
            except ActionError as error:
                st.error(str(error))
                return
        st.session_state["flash"] = ("success", f"{job_id} re-run and republished.")
        st.rerun()
    last = st.session_state.get("rerun_output")
    if last and last[0] == job_id:
        with st.expander("Output of the last re-run"):
            st.code(last[1], language="text")


def job_detail(store: dict) -> None:
    jobs = {job["job_id"]: job for job in store["jobs"]}
    job_id = st.selectbox("Job", list(jobs), format_func=lambda i: f'{i} - {jobs[i].get("name", "")}')
    job = jobs[job_id]
    files = load_job_files(ROOT, job_id)
    meta = files["meta"]

    approval = job.get("human_approval") or {}
    cells = st.columns(4)
    cells[0].metric("Status", job.get("status", ""), help=LEVEL_HELP.get(job.get("status", "")))
    cells[1].metric("Translated by", "Fixed rules" if job.get("path") == "IR" else "AI model")
    cells[2].metric("Legacy language", job.get("dialect", ""))
    cells[3].metric("Human sign-off", "Signed off" if approval.get("approved") else "Not yet")
    st.markdown(f'<div class="mf-next"><b>Next step:</b> {html.escape(next_step(job))}</div>', unsafe_allow_html=True)
    if not approval.get("approved") and approval.get("notes"):
        st.warning(approval["notes"])
    if meta.get("trap_category"):
        st.caption(f'Known trap in this job: {meta["trap_category"]}')

    code_tab, lineage_tab, report_tab, audit_tab, actions_tab = st.tabs(
        ["Code", "Lineage", "Reconciliation", "AI audit", "Review & actions"])
    with code_tab:
        code_section(job, files)
    with lineage_tab:
        lineage = load_lineage(ROOT, job_id, meta)
        st.caption(f'Method: {lineage.get("method")} | Confidence: {lineage.get("confidence")}')
        st.graphviz_chart(lineage_dot(lineage, job_id))
        outputs = [{"Output column": name, "Sources": ", ".join(info.get("sources", [])),
                    "Expression": info.get("expression", ""), "Operations": ", ".join(info.get("operations", []))}
                   for name, info in lineage.get("outputs", {}).items()]
        if outputs:
            st.dataframe(pd.DataFrame(outputs), hide_index=True, width="stretch")
        if lineage.get("candidate_source_columns"):
            st.markdown("**Candidate source columns (text match, unverified)**")
            st.write(", ".join(lineage["candidate_source_columns"]))
    with report_tab:
        report_section(load_report(UI_DIR, job_id))
    with audit_tab:
        audit_section(job)
    with actions_tab:
        actions_section(job)


# ---------- Analytics / 分析 ----------

def analytics(store: dict) -> None:
    jobs = store["jobs"]
    by_rules = [j for j in jobs if j.get("path") == "IR" and j.get("status") == "Verified"]
    by_model = [j for j in jobs if j.get("status") == "Tested"]
    timed = [j["seconds"] for j in by_rules + by_model if j.get("seconds") is not None]
    cells = st.columns(4)
    cells[0].metric("Migrated without a person writing code", f"{len(by_rules) + len(by_model)} of {len(jobs)}")
    cells[1].metric("By fixed rules", len(by_rules), help="Deterministic: the same input always gives the same code.")
    cells[2].metric("By the AI model", len(by_model))
    cells[3].metric("Average machine time per migrated job", f"{sum(timed) / len(timed):.0f} s" if timed else "-")

    st.markdown("##### Effort estimate")
    days = st.slider("Assumed days for a person to migrate and test one job by hand", 0.5, 10.0, 3.0, 0.5)
    done = len(by_rules) + len(by_model)
    st.write(f"With that assumption, the {done} migrated job(s) replace about **{done * days:g} person-days** of "
             f"hand migration; the {len(jobs) - done} remaining job(s) still need about **{(len(jobs) - done) * days:g}**. "
             "Review and sign-off time is not included.")
    st.caption("The days figure is your assumption, not a measurement. The job counts and machine times are measured. "
               "These jobs are a small sample: do not scale the share to a whole estate without sampling it first.")

    left, right = st.columns(2)
    for cell, field, title in ((left, "dialect", "By legacy language"), (right, "path", "By translation path")):
        frame = pd.DataFrame(breakdown(store, field)).T[list(LEVELS)]
        cell.markdown(f"##### {title}")
        cell.bar_chart(frame, color=["#3a9d5d", "#d9a514", "#c94f4f"])

    st.markdown("##### Why jobs still need review")
    blocked = [row for row in job_rows(store) if row["Status"] == "Needs review"]
    if not blocked:
        st.success("Every job reconciled.")
        return
    groups: dict[str, list[str]] = {}
    for row in blocked:
        groups.setdefault(blocker_type(row["Reason"]), []).append(row["Job"])
    st.dataframe(pd.DataFrame([{"Blocker": kind, "Jobs": len(ids), "Which": ", ".join(ids)}
                               for kind, ids in sorted(groups.items(), key=lambda item: -len(item[1]))]),
                 hide_index=True, width="stretch")
    with st.expander("Full reason for each job"):
        st.dataframe(pd.DataFrame([{"Job": r["Job"], "Reason": r["Reason"]} for r in blocked]),
                     hide_index=True, width="stretch")


# ---------- Try your own code / 試試自己的程式 ----------

def try_code(store: dict) -> None:
    st.write("Upload or paste a piece of legacy T-SQL or SAS to see at once how the factory would handle it. "
             "Nothing is saved, nothing is run on data, and the AI model is not called.")
    uploaded = st.file_uploader("Legacy file (.sql or .sas)", type=["sql", "sas"])
    pasted = st.text_area("...or paste T-SQL here", height=160,
                          placeholder="SELECT p.Region, COUNT(*) AS Policies FROM dbo.policy AS p GROUP BY p.Region;")
    if uploaded is not None:
        code, name = uploaded.getvalue().decode("utf-8-sig", errors="replace"), uploaded.name
    else:
        code, name = pasted, "pasted.sql"
    if not st.button("Check this code", type="primary"):
        return
    try:
        result = triage(ROOT, code, name)
    except ActionError as error:
        st.error(str(error))
        return
    if result["path"] == "IR":
        st.success("Fixed rules can translate this. Reconciled against legacy output, it would be a Verified candidate.")
        left, right = st.columns(2)
        left.markdown(f'**Legacy ({result["language"]})**')
        left.code(code, language="sql")
        right.markdown("**PySpark produced by the rules**")
        right.code(result["code"], language="python")
        if result["rules"]:
            st.markdown("**Legacy behaviours the rules reproduced**")
            for rule in result["rules"]:
                st.markdown(f"- {rule}")
    else:
        st.warning(f'Fixed rules cannot translate this: {result["reason"]}. It would go to the AI model, and could reach '
                   "Tested at best, only after its output matches legacy.")
    st.caption("Tables must exist in jobs/_schemas.yaml. To migrate it for real, add it as a folder under jobs/ and re-run.")


def main() -> None:
    st.set_page_config(page_title="Migration Factory", layout="wide", initial_sidebar_state="collapsed")
    st.markdown(STYLE, unsafe_allow_html=True)
    try:
        store = load_store(UI_DIR)
    except UiDataError as error:
        st.markdown('<div class="mf-head"><div><h1>Migration Factory</h1></div></div>', unsafe_allow_html=True)
        st.error(str(error))
        st.info("Results are not stored in the repository. Create them with:\n\n"
                "`python -m engine run-all --legacy golden --llm off`\n\n`python -m engine export`")
        return
    st.markdown('<div class="mf-head"><div><h1>Migration Factory</h1>'
                '<span class="sub">Legacy T-SQL and SAS to PySpark, checked against the legacy system\'s own output</span></div>'
                f'<span class="when">Last export: {html.escape(str(store.get("updated_at", "unknown")))}</span></div>',
                unsafe_allow_html=True)
    page = st.segmented_control("Page", PAGES, default=PAGES[0], label_visibility="collapsed", key="page") or PAGES[0]
    flash = st.session_state.pop("flash", None)
    if flash:
        (st.success if flash[0] == "success" else st.error)(flash[1])
    try:
        {"Overview": overview, "Job detail": job_detail, "Analytics": analytics, "Try your own code": try_code}[page](store)
    except UiDataError as error:
        st.error(str(error))


main()
