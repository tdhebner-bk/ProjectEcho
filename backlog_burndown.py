# backlog_burndown.py

import pandas as pd
import numpy as np
import snowflake.connector

# ------------------------------------------------------------------
# 1. SNOWFLAKE QUERY + DATA LOAD
# ------------------------------------------------------------------

BURNDOWN_SQL = """
WITH
/* ---------- Mavenlink workspaces ---------- */
ml_workspace AS (
  SELECT
      w.ID::STRING                                AS workspace_id,
      NULLIF(TRIM(w.CUSTOM_WORK_ORDER_ID), '')    AS custom_work_order_id,
      NULLIF(TRIM(w.CUSTOM_WORK_ORDER), '')       AS custom_work_order,
      NULLIF(TRIM(w.CUSTOM_PROJECT_SUB_TYPE), '') AS custom_project_sub_type
  FROM Q2_ODS.MAVENLINK.WORKSPACE w
),

/* ---------- Map workspace -> Salesforce WO (ID first, fallback name), de-dup ---------- */
ws_map_id AS (
  SELECT ms.workspace_id, wo.ID::STRING AS wo_id, wo.NAME AS wo_code, 1 AS prio
  FROM ml_workspace ms
  JOIN Q2_ODS.SALESFORCE.WORK_ORDER_C wo
    ON UPPER(TRIM(wo.ID::STRING)) = UPPER(TRIM(ms.custom_work_order_id))
),
ws_map_name AS (
  SELECT ms.workspace_id, wo.ID::STRING AS wo_id, wo.NAME AS wo_code, 2 AS prio
  FROM ml_workspace ms
  JOIN Q2_ODS.SALESFORCE.WORK_ORDER_C wo
    ON ms.custom_work_order_id IS NULL
   AND UPPER(TRIM(wo.NAME)) = UPPER(TRIM(ms.custom_work_order))
),
ws_to_wo AS (
  SELECT workspace_id, wo_id, wo_code
  FROM (
    SELECT m.*,
           ROW_NUMBER() OVER (PARTITION BY m.workspace_id ORDER BY m.prio) AS rn
    FROM (SELECT * FROM ws_map_id UNION ALL SELECT * FROM ws_map_name) m
  ) x
  WHERE rn = 1
),

/* ---------- Mavenlink: aggregate once per workspace×user (COALESCE user_id -> -1) ---------- */
time_by_ws_user AS (
  SELECT
      te.WORKSPACE_ID::STRING AS workspace_id,
      COALESCE(te.USER_ID, -1) AS user_id_num,
      (SUM(COALESCE(te.TIME_IN_MINUTES,0)) / 60.0)::NUMBER(18,2) AS hours_logged
  FROM Q2_ODS.MAVENLINK.TIME_ENTRY te
  WHERE te.approved = TRUE
  GROUP BY te.WORKSPACE_ID, COALESCE(te.USER_ID, -1)
),
alloc_by_ws_user AS (
  SELECT
      wr.WORKSPACE_ID::STRING AS workspace_id,
      COALESCE(wr.USER_ID, -1) AS user_id_num,
      (SUM(COALESCE(wa.MINUTES,0)) / 60.0)::NUMBER(18,2) AS allocated_hours
  FROM Q2_ODS.MAVENLINK.WORKSPACE_RESOURCE wr
  LEFT JOIN Q2_ODS.MAVENLINK.WORKSPACE_ALLOCATION wa
    ON wa.WORKSPACE_RESOURCE_ID = wr.ID
  WHERE wa._FIVETRAN_DELETED = FALSE
  GROUP BY wr.WORKSPACE_ID, COALESCE(wr.USER_ID, -1)
),
users_in_ws AS (
  SELECT workspace_id, user_id_num FROM alloc_by_ws_user
  UNION
  SELECT workspace_id, user_id_num FROM time_by_ws_user
),
ml_user_hours AS (
  SELECT
      u.workspace_id,
      map.wo_id,
      map.wo_code,
      u.user_id_num,
      COALESCE(a.allocated_hours, 0)::NUMBER(18,2) AS allocated_hours,
      COALESCE(t.hours_logged,    0)::NUMBER(18,2) AS hours_logged
  FROM users_in_ws u
  JOIN ws_to_wo map ON map.workspace_id = u.workspace_id
  LEFT JOIN alloc_by_ws_user a
    ON a.workspace_id = u.workspace_id AND a.user_id_num = u.user_id_num
  LEFT JOIN time_by_ws_user t
    ON t.workspace_id = u.workspace_id AND t.user_id_num = u.user_id_num
),

/* ---------- Latest Role per workspace×user ---------- */
wr_latest AS (
  SELECT
      wr.WORKSPACE_ID::STRING AS workspace_id,
      wr.USER_ID              AS user_id_num,
      COALESCE(wr.ROLE_NAME,'Unassigned') AS role_name
  FROM Q2_ODS.MAVENLINK.WORKSPACE_RESOURCE wr
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY wr.WORKSPACE_ID, wr.USER_ID
    ORDER BY wr.UPDATED_AT DESC, wr.ID DESC
  ) = 1
),

/* ---------- WOLI lists per WO (IDs, Names, Count) ---------- */
woli_lists_by_wo AS (
  SELECT
    woli.WORK_ORDER_C::STRING AS wo_id,
    COUNT(DISTINCT woli.ID)   AS woli_count,
    LISTAGG(DISTINCT woli.ID::STRING, ', ')   AS woli_ids,
    LISTAGG(DISTINCT woli.NAME, ', ')         AS woli_names
  FROM Q2_ODS.SALESFORCE.WORK_ORDER_LINE_ITEM_C woli
  GROUP BY woli.WORK_ORDER_C
),

/* ---------- Distinct Product names per WO ---------- */
products_by_wo AS (
  SELECT
    woli.WORK_ORDER_C::STRING AS wo_id,
    LISTAGG(DISTINCT p2.NAME, ', ') WITHIN GROUP (ORDER BY p2.NAME) AS product_names
  FROM Q2_ODS.SALESFORCE.WORK_ORDER_LINE_ITEM_C woli
  LEFT JOIN Q2_ODS.SALESFORCE.PRODUCT_2 p2 ON p2.ID = woli.PRODUCT_C
  GROUP BY woli.WORK_ORDER_C
),

/* ---------- Salesforce WO header + Contingent WO linkage + Record Type ---------- */
wo_base AS (
  SELECT
    wo.ID::STRING             AS wo_id,
    wo.NAME                   AS wo_code,
    wo.ACCOUNT_C              AS account_id,
    wo.OPPORTUNITY_C::STRING  AS opportunity_id,
    wo.DESCRIPTION_C          AS work_order_description,
    wo.STATUS_C               AS project_status_sf,
    wo.PROJECT_START_DATE_C   AS slotted_start_date,
    wo.REVISED_GO_LIVE_DATE_C AS slotted_go_live_date,
    wo.DELIVERY_TEAM_C        AS delivery_team,
    wo.ANALYSIS_OUTLIER_REASON_C AS analysis_outlier_reason,

    CAST(wo.PM_HOURS_BUDGETED_C               AS NUMBER(18,2)) AS pm_hours_forecast,
    CAST(wo.BC_HOURS_BUDGETED_C               AS NUMBER(18,2)) AS bc_hours_forecast,
    CAST(wo.CS_HOURS_BUDGETED_C               AS NUMBER(18,2)) AS cs_hours_forecast,
    CAST(wo.DATA_SERVICES_IE_HOURS_FORECAST_C AS NUMBER(18,2)) AS data_services_forecast,

    wo.PROJECT_MANAGER_C                  AS pm_user_id,
    wo.BUSINESS_CONSULTANT_C              AS bc_user_id,
    wo.CONFIGURATION_SPECIALIST_C         AS cs_user_id,

    /* keep as date/time per your preference */
    wo.CREATED_DATE                       AS wo_created_date,

    /* Contingent / Platform WO linkage */
    wo.PLATFORM_WO_FOR_RFA_C AS platform_wo_for_rfa_id,

    CASE
      WHEN wo.PLATFORM_WO_FOR_RFA_C IS NULL
           OR TRIM(wo.PLATFORM_WO_FOR_RFA_C) = ''
      THEN NULL
      ELSE pwo.NAME
    END AS contingent_wo_code,

    /* Contingent start date (from contingent WO) */
    CASE
      WHEN wo.PLATFORM_WO_FOR_RFA_C IS NULL
           OR TRIM(wo.PLATFORM_WO_FOR_RFA_C) = ''
      THEN NULL
      ELSE pwo.PROJECT_START_DATE_C
    END AS contingent_wo_start_date,

    /* Contingent go-live:
       1. If contingent WO has REVISED_GO_LIVE_DATE_C, use it.
       2. Else fallback to later of (today + 6 months) OR (contingent start + 6 months).
    */
    CASE
      WHEN wo.PLATFORM_WO_FOR_RFA_C IS NULL
           OR TRIM(wo.PLATFORM_WO_FOR_RFA_C) = ''
      THEN NULL

      WHEN pwo.REVISED_GO_LIVE_DATE_C IS NOT NULL
      THEN pwo.REVISED_GO_LIVE_DATE_C

      ELSE GREATEST(
             DATEADD(month, 6, CURRENT_DATE),
             DATEADD(month, 6, COALESCE(pwo.PROJECT_START_DATE_C, CURRENT_DATE))
           )
    END AS contingent_wo_go_live_date,

    /* Record Type Id forced to text + inline mapping to name */
    o.RECORD_TYPE_ID::STRING              AS opportunity_record_type_id,
    CASE o.RECORD_TYPE_ID
      WHEN '0120h000000kwUeAAI' THEN 'Q2 Gro Cross Sales Opportunity'
      WHEN '0121A000000GVD3QAO' THEN 'Helix Cross Sale Opportunity'
      WHEN '0121A000000MazmQAC' THEN 'Helix Net New Opportunity'
      WHEN '0121A000000UlN6QAK' THEN 'Centrix Cross Sales Opportunity Record Type'
      WHEN '0124X000000AZVYQA4' THEN 'Q2 Off-Platform Sales Opportunity Cross Sales'
      WHEN '0124X000001yTi5QAE' THEN 'Channel Partner Opportunity'
      WHEN '0124X000001ZWN2QAO' THEN 'PL Cross Sales Opportunity'
      WHEN '012800000003bw0AAA' THEN 'Q2 Net New Sales Opportunity'
      WHEN '012800000003Z3RAAU' THEN 'Q2 Cross Sales Opportunity Record Type'
      WHEN '012C0000000Q4NyIAK' THEN 'Renewal/Extension Opportunity Record Type'
      WHEN '012C0000000Q9x0IAC' THEN 'Amendment'
      WHEN '012C0000000QFAxIAO' THEN 'Termination Record Type'
      ELSE 'Unknown Record Type'
    END AS opportunity_record_type_name

  FROM Q2_ODS.SALESFORCE.WORK_ORDER_C wo
  LEFT JOIN Q2_ODS.SALESFORCE.OPPORTUNITY o
    ON o.ID = wo.OPPORTUNITY_C
  LEFT JOIN Q2_ODS.SALESFORCE.WORK_ORDER_C pwo
    ON pwo.NAME = wo.PLATFORM_WO_FOR_RFA_C
),

sf_account AS (
  SELECT a.ID::STRING AS account_id,
         a.NAME       AS account_name,
         a.ACCOUNT_NUMBER AS account_number
  FROM Q2_ODS.SALESFORCE.ACCOUNT a
),
sf_user AS (
  SELECT u.ID::STRING AS user_id,
         u.NAME       AS user_name
  FROM Q2_ODS.SALESFORCE.USER u
),
workspace_attrs_by_wo AS (
  SELECT
    map.wo_id,
    MAX(ms.custom_project_sub_type) AS project_sub_type
  FROM ml_workspace ms
  JOIN ws_to_wo map ON map.workspace_id = ms.workspace_id
  GROUP BY map.wo_id
)

SELECT
  /* Keys */
  wb.wo_id                                  AS "Work Order ID",
  wb.wo_code                                AS "Work Order Code",

  /* Salesforce header */
  acc.account_name                           AS "Account Name",
  acc.account_number                         AS "Account Number",
  wb.work_order_description                  AS "Work Order Description",
  wb.project_status_sf                       AS "Project Status",
  wb.slotted_start_date                      AS "Slotted Start Date",
  wb.slotted_go_live_date                    AS "Slotted Go-Live Date",
  wa.project_sub_type                        AS "Project Sub-Type",
  wb.contingent_wo_code                      AS "Contingent Work Order",
  wb.contingent_wo_go_live_date              AS "Contingent Go-Live Date",
  wb.analysis_outlier_reason                 AS "Analysis Outlier Reason",
  wb.delivery_team                           AS "Delivery Team",

  /* NEW surfaced fields */
  wb.wo_created_date                         AS "Work Order Created Date",
  wb.opportunity_id                          AS "Opportunity Id",
  wb.opportunity_record_type_id              AS "Opportunity Record Type Id",
  wb.opportunity_record_type_name            AS "Opportunity Record Type",

  /* People */
  pm.user_name                               AS "Project Manager (SF)",
  bc_user.user_name                          AS "BC (SF)",
  cs_user.user_name                          AS "Configuration Specialist (SF)",

  /* Products & WOLIs */
  pr.product_names                           AS "Product Name(s)",
  wl.woli_ids                                AS "Work Order Line Item Ids",
  wl.woli_names                              AS "Work Order Line Item Names",
  wl.woli_count                              AS "WOLI Count",

  /* Forecasts (normalized) */
  wb.pm_hours_forecast                       AS "PM Hours Forecast",
  wb.bc_hours_forecast                       AS "BC Hours Forecast",
  wb.cs_hours_forecast                       AS "IE Hours Forecast (Configuration Specialist)",
  wb.data_services_forecast                  AS "Data Services Forecast",

  /* Mavenlink per-user metrics + Role + ML user name */
  CASE
    WHEN ml.user_id_num = -1          THEN 'Unnamed Resource'
    WHEN ml.user_id_num IS NULL       THEN '—'
    ELSE mu.FULL_NAME
  END                                      AS "Mavenlink User Name",
  CASE
    WHEN ml.user_id_num = -1          THEN 'Unnamed Resource'
    WHEN ml.user_id_num IS NULL       THEN '—'
    ELSE mu.FULL_NAME
  END                                      AS "User Name",
  ml.user_id_num                           AS "Mavenlink User Id",
  CASE
    WHEN ml.user_id_num = -1          THEN 'Unnamed'
    WHEN ml.user_id_num IS NULL       THEN 'No ML Resource'
    ELSE COALESCE(wr.role_name,'Unassigned')
  END                                      AS "Role Name",
  COALESCE(ml.allocated_hours, 0)          AS "Allocated Hours (User×CWO)",
  COALESCE(ml.hours_logged,   0)           AS "Hours Logged (User×CWO)"

FROM wo_base wb
LEFT JOIN ml_user_hours ml           ON ml.wo_id       = wb.wo_id
LEFT JOIN sf_account acc             ON acc.account_id = wb.account_id
LEFT JOIN workspace_attrs_by_wo wa   ON wa.wo_id       = wb.wo_id
LEFT JOIN products_by_wo pr          ON pr.wo_id       = wb.wo_id
LEFT JOIN woli_lists_by_wo wl        ON wl.wo_id       = wb.wo_id

/* People lookups (SF) */
LEFT JOIN sf_user pm                 ON pm.user_id      = wb.pm_user_id
LEFT JOIN sf_user bc_user            ON bc_user.user_id = wb.bc_user_id
LEFT JOIN sf_user cs_user            ON cs_user.user_id = wb.cs_user_id

/* Mavenlink role + user name */
LEFT JOIN wr_latest wr               ON wr.workspace_id = ml.workspace_id
                                     AND wr.user_id_num = ml.user_id_num
LEFT JOIN Q2_ODS.MAVENLINK.USER mu   ON mu.ID           = ml.user_id_num

ORDER BY "Account Name", "Work Order Code", "Mavenlink User Name"
"""


def fetch_backlog_df():
    """Pulls the raw dataframe from Snowflake using your SQL."""
    conn = snowflake.connector.connect(
        account="Q2-Q2EDW",
        user="THEBNER",
        role="DW_IMPL_USRS",
        warehouse="Q2_WH_BI",
        database="Q2_ODS",
        schema="MAVENLINK",
        authenticator="externalbrowser",
    )
    cur = conn.cursor()
    try:
        cur.execute(BURNDOWN_SQL)
        df = cur.fetch_pandas_all()
        print("✅ Projects from Snowflake successfully pulled.")
    finally:
        cur.close()
        conn.close()
    return df


# ------------------------------------------------------------------
# 2. BUILD ALLWOs / ALLTWWOs / SHIFT MAP FROM DATAFRAME
# ------------------------------------------------------------------

def build_backlog_views(df: pd.DataFrame):
    """From raw df, build allWOs/allTWWOs/NANC and tw_shift_map and totals."""

    # Query for totals
    dtquery = '`Delivery Team` in ["CD - Wedge","CD - Product SDK"]'
    statusquery = '`Project Status` not in ["Cancelled", "Completed", "Customer Requested Cancellation", "In Question"]'
    subtypequery = '`Project Sub-Type` == "PROSERV"'

    allWOs = (
        df
        .query(f"{dtquery} and {statusquery} and {subtypequery}")
        .reset_index(drop=True)
    )
    allWOs = allWOs.rename(columns={
        "Allocated Hours (User×CWO)": "Allocated_Hours",
        "Hours Logged (User×CWO)": "Hours_Logged"
    }).reset_index()

    # Tailwind-only (CD - Wedge)
    dtTWquery = '`Delivery Team` in ["CD - Wedge"]'
    allTWWOs = (
        df
        .query(f"{dtTWquery} and {statusquery} and {subtypequery}")
        .reset_index(drop=True)
    )
    allTWWOs = allTWWOs.rename(columns={
        "Allocated Hours (User×CWO)": "Allocated_Hours",
        "Hours Logged (User×CWO)": "Hours_Logged"
    }).reset_index()

    # Non-Actionable Non-Certified (optional)
    NANC_dtquery = '`Delivery Team` in ["CD - Product SDK"]'
    NANC_statusquery = '`Project Status` in ["Pending GA"]'
    allNANCWOs = (
        df
        .query(f"{NANC_dtquery} and {NANC_statusquery} and {subtypequery}")
        .reset_index(drop=True)
    )
    allNANCWOs = allNANCWOs.rename(columns={
        "Allocated Hours (User×CWO)": "Allocated_Hours",
        "Hours Logged (User×CWO)": "Hours_Logged"
    }).reset_index()

    # Total backlog
    allocated_sum = allWOs["Allocated_Hours"].sum()
    logged_sum = allWOs["Hours_Logged"].sum()
    total_backlog = float(allocated_sum - logged_sum)

    TW_allocated_sum = allTWWOs["Allocated_Hours"].sum()
    TW_logged_sum = allTWWOs["Hours_Logged"].sum()
    total_TW_backlog = float(TW_allocated_sum - TW_logged_sum)

    
    # Build shift map (Non-Actionable Non-Certified -> Tailwind) based on Contingent Go-Live
    #
    # Important: Items with Contingent Go-Live within the next ~30 days should shift in Month 1
    # (Month 0 is the "current state" row with no flows applied).
    wo_backlog = (
    allNANCWOs
    .groupby("Work Order Code", as_index=False)
    .agg({
        "Allocated_Hours": "sum",
        "Hours_Logged": "sum",
        "Delivery Team": "first",
        "Account Name": "first",
        "Project Status": "first",
        "Work Order Description": "first",
        "Contingent Work Order": "first",
        "Contingent Go-Live Date": "first",
    })
    )

    wo_backlog["Backlog"] = wo_backlog["Allocated_Hours"] - wo_backlog["Hours_Logged"]

    # Ensure datetime
    wo_backlog["Contingent Go-Live Date"] = pd.to_datetime(
        wo_backlog["Contingent Go-Live Date"], errors="coerce"
    )

    today = pd.Timestamp.today().normalize()

    eligible = wo_backlog[
        wo_backlog["Contingent Work Order"].notna()
        & wo_backlog["Contingent Go-Live Date"].notna()
        & (wo_backlog["Backlog"] > 0)
        ].copy()

    # ---------------------------------------------------------
    # If contingent go-live date is before today, set it to the
    # first day of the month after today.
    # ---------------------------------------------------------
    first_of_next_month = (today + pd.offsets.MonthBegin(1)).normalize()

    eligible.loc[
        eligible["Contingent Go-Live Date"] < today,
        "Contingent Go-Live Date"
    ] = first_of_next_month

    # -------------------------------
    # Calendar-month bucketing (year-safe)
    # -------------------------------
    today_m = today.to_period("M")
    golive_m = eligible["Contingent Go-Live Date"].dt.to_period("M")

    # Calendar month difference: 0 = same calendar month, 1 = next month, etc.
    eligible["GoLive_Month_Offset"] = (
            golive_m.astype("int64") - today_m.ordinal
    ).astype("Int64")  # nullable integer (keeps <NA> if Go-Live is missing)

    # ---------------------------------------------------------
    # NANC export table (one row per Work Order Code)
    # ---------------------------------------------------------
    nanc_export = wo_backlog.copy()

    # Compute months-to-go-live for all NANC WOs (nullable)
    nanc_export["Months_To_GoLive"] = (
        nanc_export["Contingent Go-Live Date"]
        .dt.to_period("M")
        .astype("int64")
        .sub(today.to_period("M").ordinal)
    ).astype("Int64")

    # Final column order
    nanc_export = nanc_export[[
        "Work Order Code",
        "Account Name",
        "Project Status",
        "Work Order Description",
        "Backlog",
        "Contingent Work Order",
        "Contingent Go-Live Date",
        "Months_To_GoLive",
    ]]

    # ---------------------------------------------------------
    # ALL work orders export table (one row per Work Order Code)
    # Includes Work_Category: Tailwind / Non-Actionable / Actionable
    # ---------------------------------------------------------
    allwo_backlog = (
        allWOs
        .groupby("Work Order Code", as_index=False)
        .agg({
            "Allocated_Hours": "sum",
            "Hours_Logged": "sum",
            "Delivery Team": "first",
            "Account Name": "first",
            "Project Status": "first",
            "Work Order Description": "first",
            "Slotted Go-Live Date": "first",
            "Contingent Work Order": "first",
            "Contingent Go-Live Date": "first",
        })
    )

    allwo_backlog["Backlog"] = allwo_backlog["Allocated_Hours"] - allwo_backlog["Hours_Logged"]

    # Ensure datetime for month math
    allwo_backlog["Contingent Go-Live Date"] = pd.to_datetime(
        allwo_backlog["Contingent Go-Live Date"], errors="coerce"
    )

    # Tailwind: CD - Wedge (matches allTWWOs filter) :contentReference[oaicite:4]{index=4}
    # Non-Actionable: CD - Product SDK + Pending GA (matches allNANCWOs filter) :contentReference[oaicite:5]{index=5}
    # Actionable: everything else in allWOs :contentReference[oaicite:6]{index=6}
    allwo_backlog["Work_Category"] = np.select(
        [
            allwo_backlog["Delivery Team"].eq("CD - Wedge"),
            allwo_backlog["Delivery Team"].eq("CD - Product SDK") & allwo_backlog["Project Status"].eq("Pending GA"),
        ],
        [
            "Tailwind",
            "Non-Actionable",
        ],
        default="Actionable",
    )

    allwo_backlog["Months_To_GoLive"] = (
        allwo_backlog["Contingent Go-Live Date"]
        .dt.to_period("M")
        .astype("int64")
        .sub(today.to_period("M").ordinal)
    ).astype("Int64")

    allwo_export = allwo_backlog[[
        "Work Order Code",
        "Account Name",
        "Project Status",
        "Work Order Description",
        "Backlog",
        "Allocated_Hours",
        "Hours_Logged",
        "Slotted Go-Live Date",
        "Contingent Work Order",
        "Contingent Go-Live Date",
        "Months_To_GoLive",
        "Work_Category",
    ]]

    # Shift occurs the month AFTER the go-live month:
    # - go-live this month (0) => shift in Month 1
    # - go-live next month (1) => shift in Month 2
    eligible["Shift_Month"] = eligible["GoLive_Month_Offset"].clip(lower=0) + 1
    # Safety: ensure nothing ends up in Month 0 (model loop is 1..N)
    eligible["Shift_Month"] = eligible["Shift_Month"].clip(lower=1)

    shift_schedule = (
        eligible
        .groupby("Shift_Month", as_index=False)["Backlog"]
        .sum()
        .rename(columns={"Shift_Month": "Month", "Backlog": "Shift_Hours"})
    )

    tw_shift_map = (
        shift_schedule
        .set_index("Month")["Shift_Hours"]
        .astype(float)
        .to_dict()
    )

    # If anything somehow landed in Month 0, push it into Month 1
    if 0 in tw_shift_map:
        tw_shift_map[1] = float(tw_shift_map.get(1, 0.0)) + float(tw_shift_map.pop(0))

    return {
        "df": df,
        "allWOs": allWOs,
        "allTWWOs": allTWWOs,
        "allNANCWOs": allNANCWOs,
        "total_backlog": total_backlog,
        "total_TW_backlog": total_TW_backlog,
        "tw_shift_map": tw_shift_map,
        "shift_schedule": shift_schedule,
        "eligible": eligible,
        "nanc_export": nanc_export,
        "allwo_export": allwo_export,
    }


# ------------------------------------------------------------------
# 3. BURNDOWN MODEL
# ------------------------------------------------------------------

def run_tailwind_model(
    total_backlog: float,
    total_TW_backlog: float,
    total_NANC_backlog: float,
    tw_headcount: int,
    q2_headcount: int,
    utilization: float,
    qtr_demand_total: float,
    q2_capacity_to_q2_pct: float,
    tw_share_of_demand: float,      # fraction 0–1
    months: int,
    tw_shift_map: dict,
    removal_threshold_months: float,  # threshold in months at full capacity
    tw_capacity_reduction_month: int = 0,  # 1-based month; 0 disables reduction
    model_diff_demand_after_removal: bool = False,
    post_removal_qtr_demand_total: float | None = None,
    modify_demand_after_12_months: bool = False,
    post_12_qtr_demand_total: float | None = None,
) -> pd.DataFrame:

    # -----------------------------
    # Initialize backlog buckets
    # -----------------------------
    TW_b = float(total_TW_backlog)
    NANC_b = float(total_NANC_backlog)
    AC_b = float(total_backlog - total_TW_backlog - total_NANC_backlog)
    if AC_b < 0:
        AC_b = 0.0

    # -----------------------------
    # Capacities (monthly)
    # -----------------------------
    TW_capacity = tw_headcount * (2080 / 12) * utilization
    Q2_capacity_full = q2_headcount * (2080 / 12) * utilization  # full Q2 monthly capacity

    tailwind_active = True
    results_reduced: list[dict] = []

    # -------------------------------------------------
    # Month 0: baseline row (no burn, no incoming, no shifts)
    # -------------------------------------------------
    total_cap_m0 = TW_capacity + Q2_capacity_full
    total_b0 = TW_b + NANC_b + AC_b
    results_reduced.append({
        "Month": 0,
        "Total_Backlog": total_b0,
        "TW_Backlog": TW_b,
        "NANC_Backlog": NANC_b,
        "AC_Backlog": AC_b,
        "Total_Capacity": total_cap_m0,
        "Backlog_Months": (total_b0 / total_cap_m0) if total_cap_m0 > 0 else None,
        "Tailwind_Active": tailwind_active
    })

    # Ensure shifts occur starting Month 1 (model loop is 1..N)
    if 0 in tw_shift_map:
        tw_shift_map[1] = float(tw_shift_map.get(1, 0.0)) + float(tw_shift_map.pop(0))

    # -------------------------------------------------
    # Months 1..N: apply incoming, burn, shifts
    # -------------------------------------------------
    for month in range(1, months + 1):

        # Determine quarterly demand while Tailwind is active
        active_qtr_demand = qtr_demand_total
        if tailwind_active and modify_demand_after_12_months and month >= 13:
            active_qtr_demand = post_12_qtr_demand_total if post_12_qtr_demand_total is not None else qtr_demand_total

        # Capacities and incoming
        if tailwind_active:
            TW_cap_current = TW_capacity
            if tw_capacity_reduction_month and month >= tw_capacity_reduction_month:
                TW_cap_current *= 0.5

            # Q2 is available; split between AC vs TW per allocation slider
            Q2_cap_current = Q2_capacity_full

            TW_incoming_current = active_qtr_demand * tw_share_of_demand * (1 / 3)
            AC_incoming_current = active_qtr_demand * (1 - tw_share_of_demand) * (1 / 3)
        else:
            # Tailwind removed: TW capacity/incoming = 0, Q2 continues
            TW_cap_current = 0.0
            TW_incoming_current = 0.0

            Q2_cap_current = Q2_capacity_full

            if model_diff_demand_after_removal:
                effective_qtr = post_removal_qtr_demand_total if post_removal_qtr_demand_total is not None else qtr_demand_total
                AC_incoming_current = effective_qtr * (1 / 3)
            else:
                AC_incoming_current = 0.0

        # -----------------------------
        # Burn logic
        # -----------------------------
        if tailwind_active:
            # Q2 allocation: some capacity burns AC, remainder supports TW
            Q2_cap_to_AC = Q2_cap_current * float(q2_capacity_to_q2_pct)
            Q2_cap_to_TW = Q2_cap_current * (1.0 - float(q2_capacity_to_q2_pct))
        else:
            # After Tailwind removal, all Q2 capacity burns AC
            Q2_cap_to_AC = Q2_cap_current
            Q2_cap_to_TW = 0.0

        # NANC is NOT burned; it only shifts.

        # 1) Burn Actionable with its allocated Q2 capacity
        AC_burn = min(AC_b, Q2_cap_to_AC)
        AC_b_after_burn = AC_b - AC_burn

        # 2) If Actionable is exhausted, reallocate the unused AC capacity to Tailwind (same month)
        unused_AC_capacity = max(Q2_cap_to_AC - AC_burn, 0.0)
        effective_Q2_cap_to_TW = Q2_cap_to_TW + unused_AC_capacity

        # 3) Burn Tailwind with TW capacity + all Q2 capacity available to TW (including reallocated)
        TW_burn = min(TW_b, TW_cap_current + effective_Q2_cap_to_TW)

        # 4) Apply burn + incoming
        TW_b = max(TW_b - TW_burn + TW_incoming_current, 0.0)
        AC_b = max(AC_b_after_burn + AC_incoming_current, 0.0)

        # -----------------------------
        # Apply NANC -> Tailwind shift (Month 1+ only)
        # -----------------------------
        if tailwind_active:
            shift = float(tw_shift_map.get(month, 0.0))
            if shift > 0:
                actual_shift = min(NANC_b, shift)
                NANC_b -= actual_shift
                TW_b += actual_shift

        # -----------------------------
        # Metrics & Tailwind removal check
        # -----------------------------
        total_b = TW_b + NANC_b + AC_b
        total_cap_current = TW_cap_current + Q2_cap_current
        backlog_months = (total_b / total_cap_current) if total_cap_current > 0 else None

        if tailwind_active and (backlog_months is not None) and (backlog_months <= removal_threshold_months):
            tailwind_active = False

            # If modeling different incoming demand after removal, apply starting in threshold month
            if model_diff_demand_after_removal:
                effective_qtr = post_removal_qtr_demand_total if post_removal_qtr_demand_total is not None else qtr_demand_total
                effective_monthly = effective_qtr * (1 / 3)

                # Remove TW incoming already applied this month (post-removal TW incoming = 0)
                TW_b = max(TW_b - TW_incoming_current, 0.0)

                # Replace AC incoming for this month with post-removal incoming (if different)
                AC_b = max(AC_b + (effective_monthly - AC_incoming_current), 0.0)

            # Transfer any remaining TW backlog into AC (so Q2 can burn it going forward)
            AC_b += TW_b
            TW_b = 0.0

            # Recompute metrics with Q2-only capacity
            total_b = TW_b + NANC_b + AC_b
            total_cap_current = Q2_capacity_full
            backlog_months = (total_b / total_cap_current) if total_cap_current > 0 else None

        results_reduced.append({
            "Month": month,
            "Total_Backlog": total_b,
            "TW_Backlog": TW_b,
            "NANC_Backlog": NANC_b,
            "AC_Backlog": AC_b,
            "Total_Capacity": total_cap_current,
            "Backlog_Months": backlog_months,
            "Tailwind_Active": tailwind_active
        })

    return pd.DataFrame(results_reduced)


    df_reduced = pd.DataFrame(results_reduced)
    return df_reduced
