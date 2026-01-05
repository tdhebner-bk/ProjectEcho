# app.py

import streamlit as st
import matplotlib.pyplot as plt

import json
import re
from pathlib import Path
from typing import Optional, Tuple, List

from backlog_burndown import (
    fetch_backlog_df,
    build_backlog_views,
    run_tailwind_model,
)

st.set_page_config(page_title="Backlog Burndown (Q2 + Tailwind)", layout="wide")

# -------------------------------------------------
# Chart color management (ColorCombos.ipynb -> apdocc Combinations)
# -------------------------------------------------

def _get_combo_name_from_colorcombos(ipynb_path: Path) -> Optional[int]:
    """Read ColorCombos.ipynb and extract combo_name from the 'SPECIFIC SELECTOR' section."""
    if not ipynb_path.exists():
        return None

    try:
        nb = json.loads(ipynb_path.read_text(encoding="utf-8"))
    except Exception:
        return None

    cells = nb.get("cells", [])
    # Prefer a cell that explicitly contains the SPECIFIC SELECTOR header
    for cell in cells:
        src = "".join(cell.get("source", []))
        if "SPECIFIC SELECTOR" in src:
            m = re.search(r"\bcombo_name\s*=\s*(\d+)\b", src)
            if m:
                return int(m.group(1))

    # Fallback: first combo_name assignment anywhere
    for cell in cells:
        src = "".join(cell.get("source", []))
        m = re.search(r"\bcombo_name\s*=\s*(\d+)\b", src)
        if m:
            return int(m.group(1))

    return None


def _get_color_list_from_apdocc(combo_name: int, n_colors: int) -> Optional[List[str]]:
    """Resolve an apdocc Combinations entry into a list of hex colors.

    This mirrors the logic in the ColorCombos.ipynb 'SPECIFIC SELECTOR' section:
    - combo may be a dict (values are colors)
    - combo may have a .colors attribute (iterable of color objects)
    - combo may be a list/tuple of color objects/strings
    - otherwise treat combo as a single color-like object
    """
    try:
        import apdocc  # noqa: F401
        from apdocc import Combinations
    except Exception:
        return None

    # --- locate the combo in apdocc.Combinations ---
    combo = None
    if isinstance(Combinations, dict):
        combo = Combinations.get(combo_name)
    elif hasattr(Combinations, str(combo_name)):
        combo = getattr(Combinations, str(combo_name))
    elif hasattr(Combinations, combo_name):  # type: ignore[arg-type]
        combo = getattr(Combinations, combo_name)  # pragma: no cover

    if combo is None:
        return None

    def _to_hex(c) -> Optional[str]:
        """Best-effort conversion of an apdocc color-like object to a matplotlib-friendly color string."""
        if c is None:
            return None
        hex_val = getattr(c, "hex", None)
        if isinstance(hex_val, str) and hex_val.startswith("#"):
            return hex_val
        s = str(c)
        if isinstance(s, str) and s.startswith("#"):
            return s
        # As a last resort, let matplotlib attempt to parse common names; otherwise skip
        return s if isinstance(s, str) and s else None

    # --- normalize to a list of color objects ---
    if isinstance(combo, dict):
        raw = list(combo.values())
    elif hasattr(combo, "colors"):
        raw = list(getattr(combo, "colors"))
    elif isinstance(combo, (list, tuple)):
        raw = list(combo)
    else:
        raw = [combo]

    colors: List[str] = []
    for c in raw:
        hx = _to_hex(c)
        if hx:
            colors.append(hx)

    if not colors:
        return None

    # Ensure exactly n_colors by repeating/cropping
    while len(colors) < n_colors:
        colors = colors + colors
    return colors[:n_colors]
##[:n_colors] if colors else None


def get_chart_palette(n_colors: int = 3) -> Tuple[Optional[List[str]], Optional[int]]:
    """Return (palette, combo_name) if resolvable; otherwise (None, None)."""
    ipynb_path = Path(__file__).with_name("ColorCombos.ipynb")
    combo_name = _get_combo_name_from_colorcombos(ipynb_path)
    if combo_name is None:
        return None, None

    palette = _get_color_list_from_apdocc(combo_name, n_colors=n_colors)
    return palette, combo_name


def render_palette_preview(palette: List[str], combo_name: Optional[int]):
    """Small sidebar preview of the chosen palette."""
    label = f"Color combo: {combo_name}" if combo_name is not None else "Color combo"
    swatches = " ".join(
        [f"<span style='display:inline-block;width:14px;height:14px;border-radius:3px;background:{c};border:1px solid #999;'></span>" for c in palette]
    )
    st.sidebar.markdown(f"### Chart Colors\n{label}<br/>{swatches}", unsafe_allow_html=True)


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
        help="Used to compute monthly incoming demand starting in the Tailwind removal threshold month."
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
        modify_demand_after_12_months=modify_demand_after_12_months,
        post_12_qtr_demand_total=post_12_qtr_demand_total,
    )

    st.subheader("Burndown Results (Tailwind Removed at Backlog Threshold)")
    st.dataframe(df_reduced)

    plt.style.use("classic")

    palette3, combo_name = get_chart_palette(n_colors=3)
    if palette3:
        render_palette_preview(palette3, combo_name)


    def add_labels(ax, x, y, step=1, fmt="{:,.0f}", y_offset=0):
        for i in range(0, len(x), step):
            val = y.iloc[i] if hasattr(y, "iloc") else y[i]

            # Stop labeling once the line reaches zero or below
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

    ax1.plot(df_reduced["Month"], df_reduced["Total_Backlog"], marker='o', label="Total Backlog", color=(palette3[0] if palette3 else None))
    ax1.plot(df_reduced["Month"], df_reduced["TW_Backlog"], marker='o', label="Tailwind Backlog", color=(palette3[1] if palette3 else None))
    ax1.plot(df_reduced["Month"], df_reduced["Q2_Backlog"], marker='o', label="Q2 Backlog", color=(palette3[2] if palette3 else None))

    # Labels
    add_labels(
        ax1,
        df_reduced["Month"],
        df_reduced["Total_Backlog"],
        step=3,
        y_offset=8  # above the line
    )

    add_labels(
        ax1,
        df_reduced["Month"],
        df_reduced["TW_Backlog"],
        step=3,
        y_offset=-10  # below the line
    )

    add_labels(
        ax1,
        df_reduced["Month"],
        df_reduced["Q2_Backlog"],
        step=3,
        y_offset=8  # above the line
    )

    ax1.set_xlabel("Month")
    ax1.set_ylabel("Backlog (Hours)")
    ax1.set_title("Backlog Burndown – Tailwind Removed at Backlog Threshold")
    ax1.legend()
    ax1.grid(True)

    st.pyplot(fig1)

    # 2) Backlog in months
    fig2, ax2 = plt.subplots(figsize=(10, 5))
    ax2.plot(
        df_reduced["Month"],
        df_reduced["Backlog_Months"],
        marker="o",
        label="Backlog (Months)",
        color=(palette3[0] if palette3 else None),
    )

    add_labels(
        ax2,
        df_reduced["Month"],
        df_reduced["Backlog_Months"],
        step=3,
        y_offset=8,
        fmt="{:.1f}",
    )

    ax2.set_xlabel("Month")
    ax2.set_ylabel("Backlog (Months)")
    ax2.set_title("Backlog in Months – Tailwind Removed at Backlog Threshold")

    # ---- added axis control ----
    ax2.set_xlim(0, 30)
    ax2.set_ylim(0, 15)
    ax2.set_yticks(range(0, 16, 3))
    # ----------------------------

    ax2.legend()
    ax2.grid(True)
    st.pyplot(fig2)

else:
    st.info("Adjust parameters on the left and click 'Run Burndown Simulation'.")
