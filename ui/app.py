"""Migration Factory dashboard (Streamlit). Run from the project root: `streamlit run ui/app.py`.
Migration Factory 儀表板(Streamlit)。在專案根目錄執行:`streamlit run ui/app.py`。

Read-only: it shows what `python -m engine export` published and never changes a status or an approval.
唯讀:只顯示 `python -m engine export` 發布的結果,不會更改任何狀態或核可。
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import streamlit as st

# `streamlit run ui/app.py` puts ui/ on the path, not the project root.
# `streamlit run ui/app.py` 只會把 ui/ 放進路徑,不含專案根目錄。
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from ui.data import (LEVELS, ROOT, UiDataError, breakdown, gold_ready, job_rows, lineage_dot,  # noqa: E402
                     load_job_files, load_lineage, load_report, load_store)

UI_DIR = ROOT / "ui" / "mock_data"
STATUS_COLOURS = {"Verified": "#d7f0dd", "Tested": "#fdf0c8", "Needs review": "#f8d7d7"}
LEVEL_HELP = {
    "Verified": "Deterministic rule path; output matched legacy on the sample, adversarial and empty datasets.",
    "Tested": "AI-translated; output matched legacy on every dataset. Needs a human read before approval.",
    "Needs review": "Did not match legacy, or there is no legacy output to compare against.",
}


def colour_status(value: str) -> str:
    return f"background-color: {STATUS_COLOURS.get(value, '')}; color: #1a1a1a"


def overview(store: dict) -> None:
    metrics = store["metrics"]
    ready = gold_ready(store)
    cells = st.columns(6)
    cells[0].metric("Jobs", metrics["total_jobs"])
    cells[1].metric("Verified", metrics["verified"], help=LEVEL_HELP["Verified"])
    cells[2].metric("Tested", metrics["tested"], help=LEVEL_HELP["Tested"])
    cells[3].metric("Needs review", metrics["needs_review"], help=LEVEL_HELP["Needs review"])
    cells[4].metric("Reconciled", f'{metrics["pass_rate_pct"]}%')
    cells[5].metric("Approved for Gold", len(ready), help="Reconciled AND approved by a human reviewer.")
    st.caption(f'Last export: {store.get("updated_at", "unknown")}')

    rows = pd.DataFrame(job_rows(store))
    # Nullable integers: a missing count stays empty instead of turning the column into floats.
    # 可為空的整數:缺值維持空白,而不是讓整欄變成浮點數。
    for name in ("Discrepancies", "LLM iterations"):
        rows[name] = rows[name].astype("Int64")
    wanted = st.multiselect("Status", LEVELS, default=list(LEVELS))
    shown = rows[rows["Status"].isin(wanted)]
    st.dataframe(shown.style.map(colour_status, subset=["Status"]), hide_index=True, use_container_width=True,
                 height=35 * (len(shown) + 1) + 3)
    if not ready:
        st.warning("No job has human approval yet, so the Gold deploy will not promote anything. "
                   "A machine result never counts as an approval.")


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
    if meta.get("trap_category"):
        st.caption(f'Known trap: {meta["trap_category"]}')

    code_tab, lineage_tab, report_tab = st.tabs(["Code", "Lineage", "Reconciliation"])
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
        page = st.sidebar.radio("Page", ["Overview", "Job detail", "Analytics"])
        st.sidebar.caption("Levels")
        for level in LEVELS:
            st.sidebar.markdown(f"**{level}** - {LEVEL_HELP[level]}")
        {"Overview": overview, "Job detail": job_detail, "Analytics": analytics}[page](store)
    except UiDataError as error:
        st.error(str(error))


main()
