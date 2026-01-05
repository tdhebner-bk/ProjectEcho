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
    AND te._fivetran_deleted = FALSE
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
    # Build shift map (Non-Actionable Non-Certified -> Actionable (Tailwind/Actionable bucket))
    # We shift the *remaining* non-actionable backlog into an actionable bucket based on the
    # Contingent Go-Live Date (which already includes a 6-month fallback in the SQL).
    #
    # IMPORTANT: To ensure work that is already within 0-29 days to go-live shifts during the first
    # modeled month (Month 1), we shift by (Months_To_GoLive + 1). Month 0 is reserved for
    # "current state" output with no flow applied.

    nanc_wo_backlog = (
        allNANCWOs
        .groupby("Work Order Code", as_index=False)
        .agg({
            "Allocated_Hours": "sum",
            "Hours_Logged": "sum",
            "Delivery Team": "first",
            "Account Name": "first",
            "Work Order Description": "first",
            "Contingent Work Order": "first",
            "Contingent Go-Live Date": "first",
            "Slotted Go-Live Date": "first",
        })
    )

    nanc_wo_backlog["Backlog"] = nanc_wo_backlog["Allocated_Hours"] - nanc_wo_backlog["Hours_Logged"]
    nanc_wo_backlog["Contingent Go-Live Date"] = pd.to_datetime(
        nanc_wo_backlog["Contingent Go-Live Date"], errors="coerce"
    )

    today = pd.Timestamp.today().normalize()

    nanc_eligible = nanc_wo_backlog[
        nanc_wo_backlog["Backlog"].gt(0)
        & nanc_wo_backlog["Contingent Go-Live Date"].notna()
    ].copy()

    nanc_eligible["Days_To_GoLive"] = (nanc_eligible["Contingent Go-Live Date"] - today).dt.days
    nanc_eligible["Months_To_GoLive"] = (
        (nanc_eligible["Days_To_GoLive"] // 30)
        .clip(lower=)
        .astype(int)
    )

    # Shift in month (Months_To_GoLive + 1) so 0-month items shift in Month 1
    nanc_eligible["Shift_Month"] = nanc_eligible["Months_To_GoLive"] + 1

    shift_schedule = (
        nanc_eligible
        .groupby("Shift_Month", as_index=False)["Backlog"]
        .sum()
        .rename(columns={
            "Shift_Month": "Month",
            "Backlog": "Shift_Hours"
        })
    )

    tw_shift_map = {
        int(row["Month"]): float(row["Shift_Hours"])
        for _, row in shift_schedule.iterrows()
    }

    return {
        "df": df,
        "allWOs": allWOs,
        "allTWWOs": allTWWOs,
        "allNANCWOs": allNANCWOs,
        "total_backlog": total_backlog,
        "total_TW_backlog": total_TW_backlog,
        "tw_shift_map": tw_shift_map,
        "shift_schedule": shift_schedule,
        "eligible": nanc_eligible,
    }
# ------------------------------------------------------------------
# 3. BURNDOWN MODEL
# ------------------------------------------------------------------


def run_tailwind_model(
    total_backlog: float,
    total_TW_backlog: float,
    total_NANC_backlog: float,
    tw_headcount: float,
    q2_headcount: float,
    utilization: float,
    qtr_demand_total: float,
    tw_share_of_demand: float,      # fraction 0–1
    months: int,
    tw_shift_map: dict,
    q2_capacity_to_q2_pct: float = 1.0,  # fraction 0–1 of Q2 capacity applied to Actionable (non-TW) backlog while Tailwind is active
    removal_threshold_months: float = 12.0,  # threshold in months at full actionable capacity
    # 1-based month; 0 disables reduction
    tw_capacity_reduction_month: int = 0,
    # If True, use a different quarterly demand once Tailwind removal threshold is reached
    model_diff_demand_after_removal: bool = False,
    post_removal_qtr_demand_total: float | None = None,
    # If True, use a different quarterly demand starting in month 13 (months are 1-based)
    modify_demand_after_12_months: bool = False,
    post_12_qtr_demand_total: float | None = None,
) -> pd.DataFrame:
    """
    Burndown model with three backlog buckets:

      - TW_Backlog: Tailwind (actionable) backlog
      - NANC_Backlog: Non-Actionable Non-Certified backlog (cannot be burned until it shifts)
      - ANC_Backlog: Actionable Non-Certified backlog (actionable, non-Tailwind)

    Key mechanics:
      - Shift: NANC -> actionable bucket using tw_shift_map (derived from contingent go-live date
        w/ SQL fallback of today+6mo/start+6mo). Month 0 has no flows; Month 1 is the first period.
      - Burn:
          * Tailwind active:
              - TW burn capacity = TW team capacity + (portion of Q2 capacity allocated to TW)
              - ANC burn capacity = (portion of Q2 capacity allocated to ANC)
              - NANC is not burnable (only shifts)
          * Tailwind removed:
              - Remaining TW backlog transfers into ANC
              - Q2 burns ANC (and any future shifts from NANC land in ANC)
      - Incoming demand:
          * Incoming demand feeds actionable buckets only:
              - TW share -> TW backlog (while Tailwind active)
              - non-TW share -> ANC backlog (while Tailwind active)
              - after Tailwind removal: demand feeds ANC (either post-removal demand if enabled,
                else baseline demand incl. "after 12 months" if enabled)
    """

    # Guardrails
    total_backlog = float(total_backlog)
    total_TW_backlog = float(total_TW_backlog)
    total_NANC_backlog = float(total_NANC_backlog)

    # Initial buckets
    TW_b = max(total_TW_backlog, 0.0)
    NANC_b = max(total_NANC_backlog, 0.0)
    ANC_b = max(total_backlog - total_TW_backlog - total_NANC_backlog, 0.0)

    # Capacities (hours/month)
    TW_capacity_base = tw_headcount * (2080 / 12) * utilization
    Q2_capacity_base = q2_headcount * (2080 / 12) * utilization

    tailwind_active = True
    results = []

    for month in range(0, months + 1):
        apply_flow = month > 0

        # -----------------------------
        # Determine capacities this month
        # -----------------------------
        if tailwind_active:
            TW_cap_current = TW_capacity_base
            if tw_capacity_reduction_month and month >= tw_capacity_reduction_month:
                TW_cap_current *= 0.5
            Q2_cap_current = Q2_capacity_base
        else:
            TW_cap_current = 0.0
            Q2_cap_current = Q2_capacity_base

        # -----------------------------
        # Determine incoming demand this month
        # -----------------------------
        if not apply_flow:
            TW_incoming = 0.0
            ANC_incoming = 0.0
        else:
            # While Tailwind is active, we may swap in the post-12-months quarterly demand.
            active_qtr_demand = qtr_demand_total
            if modify_demand_after_12_months and month >= 13:
                active_qtr_demand = post_12_qtr_demand_total if post_12_qtr_demand_total is not None else qtr_demand_total

            if tailwind_active:
                TW_incoming = active_qtr_demand * tw_share_of_demand * (1 / 3)
                ANC_incoming = active_qtr_demand * (1 - tw_share_of_demand) * (1 / 3)
            else:
                # After Tailwind removal, all demand is treated as actionable and lands in ANC.
                if model_diff_demand_after_removal:
                    effective_qtr = post_removal_qtr_demand_total if post_removal_qtr_demand_total is not None else qtr_demand_total
                    ANC_incoming = effective_qtr * (1 / 3)
                else:
                    # Continue baseline demand after removal (including the post-12-month swap if enabled).
                    ANC_incoming = active_qtr_demand * (1 / 3)
                TW_incoming = 0.0

        # -----------------------------
        # Shift NANC -> actionable
        # -----------------------------
        if apply_flow:
            shift = float(tw_shift_map.get(month, 0.0) or 0.0)
            if shift > 0:
                actual_shift = min(NANC_b, shift)
                NANC_b -= actual_shift
                if tailwind_active:
                    TW_b += actual_shift
                else:
                    # Tailwind removed: shifted work becomes actionable but lands in ANC
                    ANC_b += actual_shift

        # -----------------------------
        # Burn (only actionable buckets)
        # -----------------------------
        if tailwind_active:
            q2_to_anc = Q2_cap_current * float(q2_capacity_to_q2_pct)
            q2_to_tw = Q2_cap_current - q2_to_anc

            TW_burn_cap = TW_cap_current + q2_to_tw
            ANC_burn_cap = q2_to_anc
        else:
            TW_burn_cap = 0.0
            ANC_burn_cap = Q2_cap_current

        TW_burn = min(TW_b, TW_burn_cap) if apply_flow else 0.0
        ANC_burn = min(ANC_b, ANC_burn_cap) if apply_flow else 0.0

        if apply_flow:
            TW_b = max(TW_b - TW_burn + TW_incoming, 0.0)
            ANC_b = max(ANC_b - ANC_burn + ANC_incoming, 0.0)

        # -----------------------------
        # Compute metrics & evaluate Tailwind removal threshold (actionable backlog only)
        # -----------------------------
        actionable_backlog = TW_b + ANC_b
        total_backlog_now = actionable_backlog + NANC_b
        actionable_capacity = TW_cap_current + Q2_cap_current
        backlog_months = (actionable_backlog / actionable_capacity) if actionable_capacity > 0 else None

        # Trigger Tailwind removal when actionable backlog months <= threshold
        if tailwind_active and apply_flow and (backlog_months is not None) and (backlog_months <= removal_threshold_months):
            tailwind_active = False

            # Transfer remaining TW backlog to ANC (Tailwind work becomes Q2-owned)
            ANC_b += TW_b
            TW_b = 0.0

            # Recompute with Q2-only capacity
            actionable_backlog = TW_b + ANC_b
            total_backlog_now = actionable_backlog + NANC_b
            actionable_capacity = Q2_capacity_base
            backlog_months = (actionable_backlog / actionable_capacity) if actionable_capacity > 0 else None

        results.append({
            "Month": month,
            "Total_Backlog": total_backlog_now,
            "TW_Backlog": TW_b,
            "NANC_Backlog": NANC_b,
            "ANC_Backlog": ANC_b,
            "Actionable_Backlog": actionable_backlog,
            "Total_Capacity": actionable_capacity,
            "Backlog_Months": backlog_months,
            "Tailwind_Active": tailwind_active,
        })

    return pd.DataFrame(results)
