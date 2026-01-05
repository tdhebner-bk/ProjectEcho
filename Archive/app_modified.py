# app.py

import streamlit as st
import matplotlib.pyplot as plt

from backlog_burndown_modified import (
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
tw_shift_map = views["tw_shift_map"]

# -------------------------------------------------
# 2. Sidebar – model parameters
# -------------------------------------------------

st.sidebar.markdown("### Summary")
st.sidebar.markdown(
    f"""
    **Total Backlog:** <span style="color:#58771e;"><i>{total_backlog:,.0f}  hrs</i></span>  
    **Tailwind Backlog:** <span style="color:#d1bd19;"><i>{total_TW_backlog:,.0f}  hrs</i></span>
    """,
    unsafe_allow_html=True
)

st.sidebar.header("Model Parameters")

st.sidebar.markdown("### Capacity")
tw_headcount = st.sidebar.number_input("Tailwind Headcount", min_value=0, value=6, step=1)
q2_headcount = st.sidebar.number_input("Q2 Headcount", min_value=0, value=2, step=1)
utilization = st.sidebar.number_input(
    "Utilization %", min_value=0.0, value=0.78, step=0.01
)

# Month (1-based) when Tailwind capacity is reduced by 50%. Use 0 for "never".
tw_capacity_reduction_month = st.sidebar.number_input(
    "Tailwind Capacity Reduction Month (50%)",
    min_value=0,
    value=0,
    step=1,
    help="If set to N>0, Tailwind capacity will be cut in half starting in month N."
)

st.sidebar.markdown("### Demand")
qtr_demand_total = st.sidebar.number_input(
    "Quarterly Demand (hours)", min_value=0.0, value=1440.0, step=50.0
)
tw_share_of_demand = st.sidebar.slider(
    "Tailwind Share of Demand", min_value=0.0, max_value=1.0, value=0.0, step=0.05
)

model_diff_demand_after_removal = st.sidebar.checkbox(
    "Model different incoming demand after Tailwind removal?",
    value=False
)
post_removal_qtr_demand_total = None
if model_diff_demand_after_removal:
    post_removal_qtr_demand_total = st.sidebar.number_input(
        "Quarterly Demand After Tailwind Removal (hours)",
        min_value=0.0,
        value=float(qtr_demand_total),
        step=50.0,
        help="Used to compute monthly incoming demand once the Tailwind removal threshold is reached."
    )

st.sidebar.markdown("### Output Preferences")
months = st.sidebar.number_input("Simulation Horizon (months)", min_value=1, value=28, step=1)
removal_threshold_months = st.sidebar.number_input(
    "Tailwind Removal Threshold (backlog months, at full capacity)",
    min_value=0.0,
    value=4.0,
    step=0.5
)


# -------------------------------------------------
# 3. Run model
# -------------------------------------------------

if st.button("Run Burndown Simulation"):
    df_reduced = run_tailwind_model(
        total_backlog=total_backlog,
        total_TW_backlog=total_TW_backlog,
        tw_headcount=tw_headcount,
        q2_headcount=q2_headcount,
        utilization=utilization,
        qtr_demand_total=qtr_demand_total,
        tw_share_of_demand=tw_share_of_demand,
        months=months,
        tw_shift_map=tw_shift_map,
        removal_threshold_months=removal_threshold_months,
        tw_capacity_reduction_month=tw_capacity_reduction_month,
        model_diff_demand_after_removal=model_diff_demand_after_removal,
        post_removal_qtr_demand_total=post_removal_qtr_demand_total,
    )

    st.subheader("Burndown Results (Tailwind Removed at Backlog Threshold)")
    st.dataframe(df_reduced)

    plt.style.use("ggplot")

    def add_labels(ax, x, y, step=3, fmt="{:,.0f}"):
        for i, (xx, yy) in enumerate(zip(x, y)):
            if i % step == 0:
                ax.text(xx, yy, fmt.format(yy), fontsize=8, ha='center', va='bottom')

    # 1) Backlog burndown – hours
    fig1, ax1 = plt.subplots(figsize=(10, 5))
    ax1.plot(df_reduced["Month"], df_reduced["Total_Backlog"], marker='o', label="Total Backlog")
    ax1.plot(df_reduced["Month"], df_reduced["TW_Backlog"], marker='o', label="Tailwind Backlog")
    ax1.plot(df_reduced["Month"], df_reduced["Q2_Backlog"], marker='o', label="Q2 Backlog")

    add_labels(ax1, df_reduced["Month"], df_reduced["Total_Backlog"], step=3)
    add_labels(ax1, df_reduced["Month"], df_reduced["TW_Backlog"], step=3)
    add_labels(ax1, df_reduced["Month"], df_reduced["Q2_Backlog"], step=3)

    ax1.set_xlabel("Month")
    ax1.set_ylabel("Backlog (Hours)")
    ax1.set_title("Backlog Burndown – Tailwind Removed at Backlog Threshold")
    ax1.legend()
    ax1.grid(True)
    st.pyplot(fig1)

    # 2) Backlog in months
    fig2, ax2 = plt.subplots(figsize=(10, 5))
    ax2.plot(df_reduced["Month"], df_reduced["Backlog_Months"], marker='o', label="Backlog (Months)")
    add_labels(ax2, df_reduced["Month"], df_reduced["Backlog_Months"], step=3, fmt="{:.1f}")
    ax2.set_xlabel("Month")
    ax2.set_ylabel("Backlog (Months)")
    ax2.set_title("Backlog in Months – Tailwind Removed at Backlog Threshold")
    ax2.legend()
    ax2.grid(True)
    st.pyplot(fig2)

else:
    st.info("Adjust parameters on the left and click 'Run Burndown Simulation'.")
