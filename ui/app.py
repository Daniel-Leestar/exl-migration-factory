"""Migration Factory dashboard (Streamlit). Run from the project root: `streamlit run ui/app.py`.
Migration Factory 儀表板(Streamlit)。在專案根目錄執行:`streamlit run ui/app.py`。

It shows what `python -m engine export` published. On the "Review & actions" tab of a job a person can
sign the job off, withdraw a sign-off, or re-run the job; the page itself never decides a status.
顯示 `python -m engine export` 發布的結果。在 job 的「Review & actions」分頁可以簽核、撤回簽核或重跑;
頁面本身不會決定任何狀態。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# `streamlit run ui/app.py` puts ui/ on the path, not the project root.
# `streamlit run ui/app.py` 只會把 ui/ 放進路徑,不含專案根目錄。
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ui.actions import ActionError, model_status, rerun, settings, sign_off, withdraw  # noqa: E402
from ui.data import (LEVELS, ROOT, UiDataError, breakdown, gold_ready, job_rows, lineage_dot,  # noqa: E402
                     load_job_files, load_lineage, load_report, load_store)

UI_DIR = ROOT / "ui" / "mock_data"
STATUS_COLOURS = {"Verified": "#d7f0dd", "Tested": "#fdf0c8", "Needs review": "#f8d7d7"}
LEVEL_HELP = {
    "Verified": "Deterministic rule path; output matched legacy on the sample, adversarial and empty datasets.",
    "Tested": "AI-translated; output matched legacy on every dataset. Needs a human read before approval.",
    "Needs review": "Did not match legacy, or there is no legacy output to compare against.",
}
# Columns shown by default on the overview; the rest sit behind a toggle and on the job page.
# 總覽預設顯示的欄位;其餘欄位收在開關後面,並可在 job 頁面看到。
MAIN_COLUMNS = ["Job", "Name", "Category", "Status", "Human approved", "Reason"]
COLUMN_HELP = {
    "Job / Name": "The job's id and what it computes.",
    "Category": "Kind of job: aggregation, cleansing, join and so on.",
    "Status": "Verified, Tested or Needs review (see above).",
    "Human approved": "Whether a named person has signed the job off. Required before Gold.",
    "Reason": "Why a job still needs review. Empty for passing jobs.",
    "Dialect": "Legacy language: T-SQL or SAS.",
    "Path": "IR = translated by fixed rules; LLM = needed the AI model.",
    "Reconciled": "Whether the new output matched the legacy output.",
    "Discrepancies": "Number of rows that did not match.",
    "LLM iterations": "How many times the AI model was asked (first try plus repairs).",
    "Reviewer": "Who signed the job off.",
}


def colour_status(value: str) -> str:
    return f"background-color: {STATUS_COLOURS.get(value, '')}; color: #1a1a1a"


def overview(store: dict) -> None:
    metrics = store["metrics"]
    ready = gold_ready(store)
    cells = st.columns(5)
    cells[0].metric("Jobs", metrics["total_jobs"])
    cells[1].metric("Verified", metrics["verified"], help=LEVEL_HELP["Verified"])
    cells[2].metric("Tested", metrics["tested"], help=LEVEL_HELP["Tested"])
    cells[3].metric("Needs review", metrics["needs_review"], help=LEVEL_HELP["Needs review"])
    cells[4].metric("Approved for Gold", len(ready), help="Reconciled AND signed off by a person.")
    with st.expander("What do the levels and columns mean?"):
        st.markdown("**Levels**")
        for level in LEVELS:
            st.markdown(f"- **{level}** - {LEVEL_HELP[level]}")
        st.markdown("**Columns**")
        for name, text in COLUMN_HELP.items():
            st.markdown(f"- **{name}** - {text}")

    rows = pd.DataFrame(job_rows(store))
    # Nullable integers: a missing count stays empty instead of turning the column into floats.
    # 可為空的整數:缺值維持空白,而不是讓整欄變成浮點數。
    for name in ("Discrepancies", "LLM iterations"):
        rows[name] = rows[name].astype("Int64")
    left, right = st.columns([3, 1])
    wanted = left.multiselect("Status", LEVELS, default=list(LEVELS))
    every_column = right.toggle("Show all columns", value=False)
    shown = rows[rows["Status"].isin(wanted)]
    if not every_column:
        shown = shown[MAIN_COLUMNS]
    st.dataframe(shown.style.map(colour_status, subset=["Status"]), hide_index=True, use_container_width=True,
                 height=35 * (len(shown) + 1) + 3)
    if not ready:
        st.warning("No job has been signed off yet, so the Gold deploy will not promote anything. "
                   "A machine result never counts as an approval. "
                   "Open a Verified or Tested job on the Job detail page and use the Review & actions tab.")


def report_section(report: dict | None) -> None:
    if report is None:
        st.info("No reconciliation report was exported for this job.")
        return
    if report.get("reason"):
        st.error(report["reason"])
    summary = report.get("summary") or {}
    if summary.get("legacy_rows") is None:
        st.info("Nothing was compared for this job, so there are no row or column figures to show.")
    else:
        cells = st.columns(4)
        cells[0].metric("Legacy rows", summary["legacy_rows"])
        cells[1].metric("Spark rows", summary["spark_rows"], delta=summary["count_diff"] or None)
        cells[2].metric("Mismatched rows", summary["mismatched_rows"])
        cells[3].metric("Dataset shown", report.get("dataset") or "-")
    if report.get("datasets"):
        st.markdown("**Datasets**")
        st.dataframe(pd.DataFrame(report["datasets"]), hide_index=True, use_container_width=True)
    if report.get("field_checks"):
        st.markdown("**Column totals (legacy vs Spark)**")
        st.dataframe(pd.DataFrame(report["field_checks"]), hide_index=True, use_container_width=True)
    if report.get("counterexamples"):
        st.markdown("**Counterexamples** (synthetic or sample rows; at most 3)")
        st.json(report["counterexamples"], expanded=True)
    if report.get("shrinking_metadata"):
        meta = report["shrinking_metadata"]
        st.markdown(f'**Smallest failing input**: shrunk from {meta.get("original_input_rows")} to '
                    f'{meta.get("shrunk_input_rows")} input rows in {meta.get("reruns")} re-runs.')
        st.json({k: meta.get(k) for k in ("inputs", "legacy_output", "spark_output")}, expanded=False)


def job_detail(store: dict) -> None:
    jobs = {job["job_id"]: job for job in store["jobs"]}
    job_id = st.selectbox("Job", list(jobs), format_func=lambda i: f'{i} - {jobs[i].get("name", "")}')
    job = jobs[job_id]
    files = load_job_files(ROOT, job_id)
    meta = files["meta"]

    approval = job.get("human_approval") or {}
    cells = st.columns(4)
    cells[0].metric("Status", job.get("status", ""), help=LEVEL_HELP.get(job.get("status", "")))
    cells[1].metric("Path", job.get("path", ""))
    cells[2].metric("Dialect", job.get("dialect", ""))
    cells[3].metric("Human approval", "Approved" if approval.get("approved") else "Not approved")
    if approval.get("approved"):
        st.success(f'Approved by {approval.get("reviewer")} at {approval.get("timestamp")}. {approval.get("notes", "")}')
    elif approval.get("notes"):
        st.warning(approval["notes"])
    if meta.get("trap_category"):
        st.caption(f'Known trap: {meta["trap_category"]}')

    code_tab, lineage_tab, report_tab, actions_tab = st.tabs(["Code", "Lineage", "Reconciliation", "Review & actions"])
    with code_tab:
        left, right = st.columns(2)
        left.markdown(f'**Legacy ({job.get("dialect", "")})**')
        left.code(files["legacy"], language="sql")  # no SAS highlighter; SQL colouring is close enough
        right.markdown("**PySpark**")
        if files["generated"] is None:
            right.info("No migrated code is available for this job in this checkout.")
        else:
            right.caption(files["generated_origin"])
            right.code(files["generated"], language="python")
    with lineage_tab:
        lineage = load_lineage(ROOT, job_id, meta)
        st.caption(f'Method: {lineage.get("method")} | Confidence: {lineage.get("confidence")}')
        st.graphviz_chart(lineage_dot(lineage, job_id))
        outputs = [{"Output column": name, "Sources": ", ".join(info.get("sources", [])),
                    "Expression": info.get("expression", ""), "Operations": ", ".join(info.get("operations", []))}
                   for name, info in lineage.get("outputs", {}).items()]
        if outputs:
            st.dataframe(pd.DataFrame(outputs), hide_index=True, use_container_width=True)
        if lineage.get("candidate_source_columns"):
            st.markdown("**Candidate source columns (text match, unverified)**")
            st.write(", ".join(lineage["candidate_source_columns"]))
    with report_tab:
        report_section(load_report(UI_DIR, job_id))
    with actions_tab:
        actions_section(job)


@st.cache_data(ttl=15, show_spinner=False)
def cached_model_status(host: str, model: str) -> tuple[bool, str]:
    # Cached briefly so an unreachable server does not slow down every click.
    # 短暫快取,避免連不上伺服器時每次點擊都變慢。
    return model_status(host, model)


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


def analytics(store: dict) -> None:
    left, right = st.columns(2)
    for cell, field, title in ((left, "dialect", "By source dialect"), (right, "path", "By migration path")):
        frame = pd.DataFrame(breakdown(store, field)).T[list(LEVELS)]
        cell.markdown(f"**{title}**")
        cell.bar_chart(frame, color=["#3a9d5d", "#d9a514", "#c94f4f"])
        cell.dataframe(frame, use_container_width=True)

    st.markdown("**Why jobs still need review**")
    blocked = [{"Job": r["Job"], "Path": r["Path"], "LLM iterations": r["LLM iterations"],
                "Discrepancies": r["Discrepancies"], "Reason": r["Reason"]}
               for r in job_rows(store) if r["Status"] == "Needs review"]
    if blocked:
        st.dataframe(pd.DataFrame(blocked), hide_index=True, use_container_width=True)
    else:
        st.success("Every job reconciled.")


def main() -> None:
    st.set_page_config(page_title="Migration Factory", layout="wide")
    st.title("Migration Factory - legacy to PySpark")
    try:
        store = load_store(UI_DIR)
        page = st.sidebar.radio("Page", ["Overview", "Job detail", "Analytics"], label_visibility="collapsed")
        st.sidebar.divider()
        st.sidebar.caption(f'Last export: {store.get("updated_at", "unknown")}')
        flash = st.session_state.pop("flash", None)
        if flash:
            (st.success if flash[0] == "success" else st.error)(flash[1])
        {"Overview": overview, "Job detail": job_detail, "Analytics": analytics}[page](store)
    except UiDataError as error:
        st.error(str(error))


main()
