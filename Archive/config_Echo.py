import pandas as pd
import numpy as np
import snowflake.connector

#  Connect to Snowflake
conn = snowflake.connector.connect(
    account="Q2-Q2EDW",
    user="THEBNER",
    role="DW_IMPL_USRS",
    warehouse="Q2_WH_BI",
    database="Q2_ODS",
    schema="MAVENLINK",
    authenticator="externalbrowser",
)

# Run SQL and export to Excel
cur = conn.cursor()
try:
    sql = """
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

-- Optional filter you had before:
-- WHERE "Work Order Code" = 'WO-963371'

ORDER BY "Account Name", "Work Order Code", "Mavenlink User Name"
    """

    cur.execute(sql)

    # Fetch all results into a pandas DataFrame
    df = cur.fetch_pandas_all()
    # Save DataFrame to Excel
    #output_path = r"C:\Users\thebner\OneDrive - Q2e\ProjEchoHrs.xlsx"
    #df.to_excel(output_path, index=False)

    print(f"✅ Projects from Snowflake successfully pulled.")

finally:
    cur.close()
    conn.close()

## Query for totals
dtquery = '`Delivery Team` in ["CD - Wedge","CD - Product SDK"]'
statusquery = '`Project Status` not in ["Cancelled", "Completed", "Customer Requested Cancellation", "In Question"]'
subtypequery = '`Project Sub-Type` == "PROSERV"'
## can use below for testing specific WOs, just include in the query
## woquery = '`Work Order Code` == "WO-726971"'

allWOs = (
    df
    .query(f"{dtquery} and {statusquery} and {subtypequery}")
    .reset_index(drop=True)
)
## below resets names to make them more friendly
allWOs = allWOs.rename(columns={
    "Allocated Hours (User×CWO)": "Allocated_Hours",
    "Hours Logged (User×CWO)": "Hours_Logged"
}).reset_index()

## Query for Tailwinds Specific
dtTWquery = '`Delivery Team` in ["CD - Wedge"]'
## can use below for testing specific WOs, just include in the query
## woquery = '`Work Order Code` == "WO-726971"'
allTWWOs = (
    df
    .query(f"{dtTWquery} and {statusquery} and {subtypequery}")
    .reset_index(drop=True)
)
## below resets names to make them more friendly
allTWWOs = allTWWOs.rename(columns={
    "Allocated Hours (User×CWO)": "Allocated_Hours",
    "Hours Logged (User×CWO)": "Hours_Logged"
}).reset_index()
## Query for Non-Actionable Non-Certified
NANC_dtquery = '`Delivery Team` in ["CD - Product SDK"]'
NANC_statusquery = '`Project Status` in ["Pending GA"]'
allNANCWOs = (
    df
    .query(f"{NANC_dtquery} and {NANC_statusquery} and {subtypequery}")
    .reset_index(drop=True)
)
## below resets names to make them more friendly
allNANCWOs = allNANCWOs.rename(columns={
    "Allocated Hours (User×CWO)": "Allocated_Hours",
    "Hours Logged (User×CWO)": "Hours_Logged"
}).reset_index()