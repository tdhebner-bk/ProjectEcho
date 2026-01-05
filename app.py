# app.py

import streamlit as st
import matplotlib.pyplot as plt
import pandas as pd
import os
from datetime import datetime

from backlog_burndown import (
    fetch_backlog_df,
    build_backlog_views,
    run_tailwind_model,
)

st.set_page_config(page_title="Backlog Burndown (Q2 + Tailwind)", layout="wide")

st.title("Backlog Burndown Simulator (Q2 + Tailwind, Live Snowflake Data)")

# -------------------------------------------------
# 1. Load data from Snowflake (cached)
# -------------------------------------------------

@st.cache_data(show_spinner=True)
def load_data_from_snowflake():
    df = fetch_backlog_df()
    views = build_backlog_views(df)
    return df, views

if st.button("Load / Refresh data from Snowflake"):
    st.cache_data.clear()

df, views = load_data_from_snowflake()

st.success("Snowflake data loaded.")
st.caption(
    f"{len(df):,} rows returned from Snowflake; "
    f"{len(views['allWOs']):,} PROSERV WOs in scope."
)

# Unpack views
allWOs = views["allWOs"]
allTWWOs = views["allTWWOs"]
total_backlog = views["total_backlog"]
total_TW_backlog = views["total_TW_backlog"]
total_NANC_backlog = float(views["allNANCWOs"]["Allocated_Hours"].sum() - views["allNANCWOs"]["Hours_Logged"].sum())
total_AC_backlog = float(views["total_backlog"]-views["total_TW_backlog"]-total_NANC_backlog)
tw_shift_map = views["tw_shift_map"]

# -------------------------------------------------
# 2. Sidebar – model parameters
# -------------------------------------------------

st.sidebar.markdown("### Summary")
st.sidebar.markdown(
    f"""
    **Total Backlog:** <span style="color:#0057ba;"><i>{total_backlog:,.0f}  hrs</i></span>     
    **Tailwind Backlog:** <span style="color:#759243;"><i>{total_TW_backlog:,.0f}  hrs</i></span>    
    **Non-Actionable Non-Certified Backlog:** <span style="color:#ff3319;"><i>{total_NANC_backlog:,.0f}  hrs</i></span>   
    **Actionable Non-Certified Backlog:** <span style="color:#abf5ed;"><i>{total_AC_backlog:,.0f}  hrs</i></span>
    """,
    unsafe_allow_html=True
)

st.sidebar.header("Model Parameters")

st.sidebar.markdown("### Capacity")
tw_headcount = st.sidebar.number_input("Tailwind Headcount (FTE)", min_value=0.0, value=6.0, step=0.1)
q2_headcount = st.sidebar.number_input("Q2 Headcount (FTE)", min_value=0.0, value=2.0, step=0.1)
utilization = st.sidebar.number_input(
    "Utilization %", min_value=0.0, value=0.78, step=0.01
)

q2_capacity_to_q2_pct = st.sidebar.slider(
    "Q2 Capacity Allocation to Actionable Non-Certified Backlog",
    min_value=0.0,
    max_value=1.0,
    value=1.0,
    step=0.05,
    help="When Tailwind is active, this % of Q2 capacity burns Actionable Non-Certified Backlog; the remainder burns Tailwind backlog."
)

st.sidebar.markdown("### Demand")
qtr_demand_total = st.sidebar.number_input(
    "Quarterly Demand (hours)", min_value=0.0, value=1440.0, step=50.0
)
tw_share_of_demand = st.sidebar.slider(
    "Tailwind Share of Demand", min_value=0.0, max_value=1.0, value=0.0, step=0.05
)

# Month (1-based) when Tailwind capacity is reduced by 50%. Use 0 for "never".
tw_capacity_reduction_month = st.sidebar.number_input(
    "Tailwind Capacity Reduction Month (50%)",
    min_value=0,
    value=0,
    step=1,
    help="If set to N>0, Tailwind capacity will be cut in half starting in month N."
)


modify_demand_after_12_months = st.sidebar.checkbox(
    "Modify incoming demand after 12 months?",
    value=False
)
post_12_qtr_demand_total = None
if modify_demand_after_12_months:
    post_12_qtr_demand_total = st.sidebar.number_input(
        "Quarterly Demand After 12 Months (hours)",
        min_value=0.0,
        value=float(qtr_demand_total),
        step=50.0,
        help="Used to compute monthly incoming demand starting in month 13, until Tailwind removal (if applicable)."
    )

removal_threshold_months = st.sidebar.number_input(
    "Tailwind Removal Threshold (backlog months, at full capacity)",
    min_value=0.0,
    value=4.0,
    step=0.1
)

model_diff_demand_after_removal = st.sidebar.checkbox(
    "Model different incoming demand after full Tailwind removal?",
    value=False
)
post_removal_qtr_demand_total = None
if model_diff_demand_after_removal:
    post_removal_qtr_demand_total = st.sidebar.number_input(
        "Quarterly Demand After Tailwind Removal (hours)",
        min_value=0.0,
        value=float(qtr_demand_total),
        step=50.0,
        help="Used to compute monthly incoming demand starting in the Tailwind removal threshold month."
    )

st.sidebar.markdown("### Output Preferences")
months = st.sidebar.number_input("Simulation Horizon (months)", min_value=1, value=28, step=1)



# -------------------------------------------------
# 3. Run model
# -------------------------------------------------
if st.button("Run Burndown Simulation"):
    df_reduced = run_tailwind_model(
        total_backlog=total_backlog,
        total_TW_backlog=total_TW_backlog,
        total_NANC_backlog=total_NANC_backlog,
        tw_headcount=tw_headcount,
        q2_headcount=q2_headcount,
        utilization=utilization,
        q2_capacity_to_q2_pct=q2_capacity_to_q2_pct,
        qtr_demand_total=qtr_demand_total,
        tw_share_of_demand=tw_share_of_demand,
        months=months,
        tw_shift_map=tw_shift_map,
        removal_threshold_months=removal_threshold_months,
        tw_capacity_reduction_month=tw_capacity_reduction_month,
        model_diff_demand_after_removal=model_diff_demand_after_removal,
        post_removal_qtr_demand_total=post_removal_qtr_demand_total,
        modify_demand_after_12_months=modify_demand_after_12_months,
        post_12_qtr_demand_total=post_12_qtr_demand_total,
    )

    # -------------------------------------------------
    # 3a. Persist outputs (tables + charts) to disk
    # -------------------------------------------------
    base_output_dir = r"C:\Users\thebner\OneDrive - Q2e\Project Echo Simulations"
    run_id = f"ProjectEcho_BacklogSim_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    output_dir = os.path.join(base_output_dir, run_id)
    os.makedirs(output_dir, exist_ok=True)

    # -------------------------------------------------
    # 3a-i. Persist model input parameters to text file
    # -------------------------------------------------
    params_txt_path = os.path.join(
        output_dir,
        f"parameters_{run_id}.txt"
    )

    params_text = f"""
    Project Echo – Backlog Burndown Simulation
    =========================================
    Run Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

    --- Capacity Assumptions ---
    Tailwind Headcount (FTE): {tw_headcount}
    Q2 Headcount (FTE): {q2_headcount}
    Utilization Rate: {utilization:.2%}
    Q2 Capacity Allocation to Actionable: {q2_capacity_to_q2_pct:.2%}

    --- Demand Assumptions ---
    Quarterly Demand (hours): {qtr_demand_total:,.0f}
    Tailwind Share of Demand: {tw_share_of_demand:.2%}

    Modify Demand After 12 Months: {modify_demand_after_12_months}
    Quarterly Demand After 12 Months: {post_12_qtr_demand_total if post_12_qtr_demand_total is not None else 'N/A'}

    Model Different Demand After Tailwind Removal: {model_diff_demand_after_removal}
    Quarterly Demand After Tailwind Removal: {post_removal_qtr_demand_total if post_removal_qtr_demand_total is not None else 'N/A'}

    --- Structural Controls ---
    Tailwind Capacity Reduction Month (50%): {tw_capacity_reduction_month}
    Tailwind Removal Threshold (Backlog Months): {removal_threshold_months}

    --- Simulation Controls ---
    Simulation Horizon (months): {months}

    --- Initial Backlog Snapshot ---
    Total Backlog (hrs): {total_backlog:,.0f}
    Tailwind Backlog (hrs): {total_TW_backlog:,.0f}
    Non-Actionable Backlog (hrs): {total_NANC_backlog:,.0f}
    Actionable Backlog (hrs): {total_AC_backlog:,.0f}
    """

    with open(params_txt_path, "w", encoding="utf-8") as f:
        f.write(params_text.strip())

    st.success(f"Saved model parameters to: {params_txt_path}")

    # Save the main results table
    df_reduced.to_csv(
        os.path.join(output_dir, f"burndown_results_{run_id}.csv"),
        index=False
    )

    st.subheader("Burndown Results (Tailwind Removed at Backlog Threshold)")
    st.dataframe(df_reduced)

    plt.style.use("classic")

    def add_labels(ax, x, y, step=1, fmt="{:,.0f}", y_offset=0):
        for i in range(0, len(x), step):
            val = y.iloc[i] if hasattr(y, "iloc") else y[i]
            if val <= 0:
                break
            ax.annotate(
                fmt.format(val),
                (x.iloc[i] if hasattr(x, "iloc") else x[i], val),
                textcoords="offset points",
                xytext=(0, y_offset),
                ha="center",
                fontsize=9,
            )

    # 1) Backlog burndown – hours
    fig1, ax1 = plt.subplots(figsize=(10, 5))
    ax1.plot(df_reduced["Month"], df_reduced["Total_Backlog"], marker="o", label="Total Backlog")
    ax1.plot(df_reduced["Month"], df_reduced["TW_Backlog"], marker="o", label="Tailwind Backlog")
    ax1.plot(df_reduced["Month"], df_reduced["NANC_Backlog"], marker="o", label="Non-Actionable")
    ax1.plot(df_reduced["Month"], df_reduced["AC_Backlog"], marker="o", label="Actionable (Q2)")

    add_labels(ax1, df_reduced["Month"], df_reduced["Total_Backlog"], step=3, y_offset=8)
    add_labels(ax1, df_reduced["Month"], df_reduced["TW_Backlog"], step=3, y_offset=-10)
    add_labels(ax1, df_reduced["Month"], df_reduced["NANC_Backlog"], step=3, y_offset=8)
    add_labels(ax1, df_reduced["Month"], df_reduced["AC_Backlog"], step=3, y_offset=8)

    import matplotlib.ticker as ticker
    ax1.set_xlabel("Month")
    ax1.set_ylabel("Backlog (Hours)")
    ax1.yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:,.0f}"))
    ax1.set_title("Backlog Burndown – Tailwind Removed at Backlog Threshold")
    ax1.legend()
    ax1.grid(True)

    st.pyplot(fig1)
    fig1.savefig(os.path.join(output_dir, f"backlog_burndown_hours_{run_id}.png"), dpi=200, bbox_inches="tight")

    # 2) Backlog in months
    fig2, ax2 = plt.subplots(figsize=(10, 5))
    ax2.plot(df_reduced["Month"], df_reduced["Backlog_Months"], marker="o", label="Backlog (Months)")
    add_labels(ax2, df_reduced["Month"], df_reduced["Backlog_Months"], step=2, y_offset=8, fmt="{:.1f}")

    ax2.set_xlabel("Month")
    ax2.set_ylabel("Backlog (Months)")
    ax2.set_title("Backlog in Months – Tailwind Removed at Backlog Threshold")
    ax2.set_xlim(0, 30)
    ax2.set_ylim(0, 15)
    ax2.set_yticks(range(0, 16, 2))
    ax2.legend()
    ax2.grid(True)

    st.pyplot(fig2)
    fig2.savefig(os.path.join(output_dir, f"backlog_burndown_months_{run_id}.png"), dpi=200, bbox_inches="tight")

    st.success(f"Saved outputs to: {output_dir}")

    # -------------------------------------------------
    # 3b. Export NANC work orders to Excel (OneDrive root)
    # -------------------------------------------------
    nanc_base_dir = output_dir
    nanc_run_id = f"NANC_WorkShift_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    nanc_path = os.path.join(nanc_base_dir, f"{nanc_run_id}.xlsx")
    views = build_backlog_views(df)
    # views must be in scope; this expects you already have `views = build_backlog_views(df)`
    nanc_df = views.get("nanc_export")
    if nanc_df is not None and len(nanc_df) > 0:
        nanc_df.to_excel(nanc_path, index=False)
        st.success(f"Saved NANC work orders to: {nanc_path}")
    else:
        st.info("No NANC work orders found to export.")

    # -------------------------------------------------
    # 3c. Export ALL work orders to Excel (OneDrive root)
    # -------------------------------------------------
    allwo_base_dir = output_dir
    allwo_run_id = f"AllWorkOrders_WorkShift_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    allwo_path = os.path.join(allwo_base_dir, f"{allwo_run_id}.xlsx")

    allwo_df = views.get("allwo_export")
    if allwo_df is not None and len(allwo_df) > 0:
        allwo_df.to_excel(allwo_path, index=False)
        st.success(f"Saved ALL work orders export to: {allwo_path}")
    else:
        st.info("No work orders found to export in allwo_export.")

    # -------------------------------------------------
    # 3d. Shift schedule table (hours shifting by month)
    # -------------------------------------------------
    st.subheader("Hours Shifting to Tailwind by Month")

    if isinstance(tw_shift_map, dict) and len(tw_shift_map) > 0:
        shift_rows = [
            {"Month": m, "Shift_Hours": float(tw_shift_map.get(m, 0.0))}
            for m in range(1, int(months) + 1)
        ]
        shift_df = pd.DataFrame(shift_rows)

        pd.DataFrame(shift_rows).to_csv(
            os.path.join(output_dir, f"shift_schedule_full_{run_id}.csv"),
            index=False
        )

        shift_df.to_csv(
            os.path.join(output_dir, f"shift_schedule_displayed_{run_id}.csv"),
            index=False
        )

        st.dataframe(shift_df, use_container_width=True, hide_index=True)
    else:
        st.info("No contingent go-live shifts detected in the current dataset.")

else:
    st.info("Adjust parameters on the left and click 'Run Burndown Simulation'.")
