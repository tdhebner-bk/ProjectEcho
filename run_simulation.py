# run_simulation.py
#
# Headless version of app.py's "Run Burndown Simulation" button, for the
# "Project Echo Weekly" scheduled task. Pulls live Snowflake data, runs the
# model with the app's default sidebar parameters, and writes the same files
# to the same OneDrive folder the app does.
#
# Keep PARAMS in step with the sidebar defaults in app.py.

import os
import sys
from datetime import datetime

import matplotlib
matplotlib.use("Agg")  # no display under Task Scheduler
import matplotlib.pyplot as plt
import matplotlib.ticker as ticker
import pandas as pd

from backlog_burndown import (
    fetch_backlog_df,
    build_backlog_views,
    run_tailwind_model,
)

BASE_OUTPUT_DIR = r"C:\Users\thebner\OneDrive - Q2e\Project Echo Simulations"

PARAMS = dict(
    tw_headcount=6.0,
    q2_headcount=2.0,
    utilization=0.78,
    q2_capacity_to_q2_pct=0.4,
    rollover_demand=440.0,
    new_sales_demand=1000.0,
    tw_share_of_demand=0.75,
    tw_capacity_reduction_month=12,
    modify_demand_after_12_months=False,
    post_12_qtr_demand_total=None,
    removal_threshold_months=4.0,
    model_diff_demand_after_removal=False,
    post_removal_qtr_demand_total=None,
    months=28,
)


def add_labels(ax, x, y, step=1, fmt="{:,.0f}", y_offset=0):
    for i in range(0, len(x), step):
        val = y.iloc[i]
        if val <= 0:
            break
        ax.annotate(
            fmt.format(val),
            (x.iloc[i], val),
            textcoords="offset points",
            xytext=(0, y_offset),
            ha="center",
            fontsize=9,
        )


def main():
    p = PARAMS
    qtr_demand_total = p["rollover_demand"] + p["new_sales_demand"]

    print("Pulling backlog from Snowflake...", flush=True)
    df = fetch_backlog_df()
    views = build_backlog_views(df)
    print(f"{len(df):,} rows; {len(views['allWOs']):,} PROSERV WOs in scope.", flush=True)

    total_backlog = views["total_backlog"]
    total_TW_backlog = views["total_TW_backlog"]
    total_NANC_backlog = float(views["allNANCWOs"]["Allocated_Hours"].sum() - views["allNANCWOs"]["Hours_Logged"].sum())
    total_AC_backlog = float(total_backlog - total_TW_backlog - total_NANC_backlog)
    tw_shift_map = views["tw_shift_map"]

    df_reduced = run_tailwind_model(
        total_backlog=total_backlog,
        total_TW_backlog=total_TW_backlog,
        total_NANC_backlog=total_NANC_backlog,
        tw_headcount=p["tw_headcount"],
        q2_headcount=p["q2_headcount"],
        utilization=p["utilization"],
        q2_capacity_to_q2_pct=p["q2_capacity_to_q2_pct"],
        qtr_demand_total=qtr_demand_total,
        tw_share_of_demand=p["tw_share_of_demand"],
        months=p["months"],
        tw_shift_map=tw_shift_map,
        removal_threshold_months=p["removal_threshold_months"],
        tw_capacity_reduction_month=p["tw_capacity_reduction_month"],
        model_diff_demand_after_removal=p["model_diff_demand_after_removal"],
        post_removal_qtr_demand_total=p["post_removal_qtr_demand_total"],
        modify_demand_after_12_months=p["modify_demand_after_12_months"],
        post_12_qtr_demand_total=p["post_12_qtr_demand_total"],
    )

    run_id = f"ProjectEcho_BacklogSim_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    output_dir = os.path.join(BASE_OUTPUT_DIR, run_id)
    os.makedirs(output_dir, exist_ok=True)

    # Parameters file -- same layout as app.py
    post_12 = p["post_12_qtr_demand_total"]
    post_removal = p["post_removal_qtr_demand_total"]
    params_text = f"""
    Project Echo – Backlog Burndown Simulation
    =========================================
    Run Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
    Run Source: scheduled task (run_simulation.py, default parameters)

    --- Capacity Assumptions ---
    Tailwind Headcount (FTE): {p['tw_headcount']}
    Q2 Headcount (FTE): {p['q2_headcount']}
    Utilization Rate: {p['utilization']:.2%}
    Q2 Capacity Allocation to Actionable: {p['q2_capacity_to_q2_pct']:.2%}

    --- Demand Assumptions ---
    Quarterly Demand (hours): {qtr_demand_total:,.0f}
    Tailwind Share of Demand: {p['tw_share_of_demand']:.2%}

    Modify Demand After 12 Months: {p['modify_demand_after_12_months']}
    Quarterly Demand After 12 Months: {post_12 if post_12 is not None else 'N/A'}

    Model Different Demand After Tailwind Removal: {p['model_diff_demand_after_removal']}
    Quarterly Demand After Tailwind Removal: {post_removal if post_removal is not None else 'N/A'}

    --- Structural Controls ---
    Tailwind Capacity Reduction Month (50%): {p['tw_capacity_reduction_month']}
    Tailwind Removal Threshold (Backlog Months): {p['removal_threshold_months']}

    --- Simulation Controls ---
    Simulation Horizon (months): {p['months']}

    --- Initial Backlog Snapshot ---
    Total Backlog (hrs): {total_backlog:,.0f}
    Tailwind Backlog (hrs): {total_TW_backlog:,.0f}
    Non-Actionable Backlog (hrs): {total_NANC_backlog:,.0f}
    Actionable Backlog (hrs): {total_AC_backlog:,.0f}
    """
    with open(os.path.join(output_dir, f"parameters_{run_id}.txt"), "w", encoding="utf-8") as f:
        f.write(params_text.strip())

    df_reduced.to_csv(os.path.join(output_dir, f"burndown_results_{run_id}.csv"), index=False)

    plt.style.use("classic")

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
    ax1.set_xlabel("Month")
    ax1.set_ylabel("Backlog (Hours)")
    ax1.yaxis.set_major_formatter(ticker.StrMethodFormatter("{x:,.0f}"))
    ax1.set_title("Backlog Burndown – Tailwind Removed at Backlog Threshold")
    ax1.legend()
    ax1.grid(True)
    fig1.savefig(os.path.join(output_dir, f"backlog_burndown_hours_{run_id}.png"), dpi=200, bbox_inches="tight")
    plt.close(fig1)

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
    fig2.savefig(os.path.join(output_dir, f"backlog_burndown_months_{run_id}.png"), dpi=200, bbox_inches="tight")
    plt.close(fig2)

    # Work order exports
    stamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    nanc_df = views.get("nanc_export")
    if nanc_df is not None and len(nanc_df) > 0:
        nanc_df.to_excel(os.path.join(output_dir, f"NANC_WorkShift_{stamp}.xlsx"), index=False)
    else:
        print("No NANC work orders found to export.")

    allwo_df = views.get("allwo_export")
    if allwo_df is not None and len(allwo_df) > 0:
        allwo_df.to_excel(os.path.join(output_dir, f"AllWorkOrders_WorkShift_{stamp}.xlsx"), index=False)
    else:
        print("No work orders found to export in allwo_export.")

    # Shift schedule
    if isinstance(tw_shift_map, dict) and len(tw_shift_map) > 0:
        shift_df = pd.DataFrame([
            {"Month": m, "Shift_Hours": float(tw_shift_map.get(m, 0.0))}
            for m in range(1, int(p["months"]) + 1)
        ])
        shift_df.to_csv(os.path.join(output_dir, f"shift_schedule_full_{run_id}.csv"), index=False)
        shift_df.to_csv(os.path.join(output_dir, f"shift_schedule_displayed_{run_id}.csv"), index=False)
    else:
        print("No contingent go-live shifts detected in the current dataset.")

    print(f"Saved outputs to: {output_dir}", flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
