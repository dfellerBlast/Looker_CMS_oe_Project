view: oe_oetodate_2022_ua {
  derived_table: {
    sql:--OE to date
DECLARE CURRENTYEAR_START STRING DEFAULT '20211015';
DECLARE CURRENTYEAR_END STRING DEFAULT '20211207';
DECLARE PREVIOUSYEAR_START STRING DEFAULT '20201015';
DECLARE PREVIOUSYEAR_END STRING DEFAULT '20201207';

WITH plan_compare AS (
  SELECT fullVisitorId, visitId, CONCAT(fullVisitorId, visitId, date) AS sessionId, CAST(hits.type = 'PAGE' AS INT64) AS pageview
  ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', date)) AS year
  ,EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', date)) AS Week
  FROM `steady-cat-772.30876903.ga_sessions_*`
  ,UNNEST(hits) AS hits
  WHERE (_TABLE_SUFFIX BETWEEN CURRENTYEAR_START AND CURRENTYEAR_END OR _TABLE_SUFFIX BETWEEN PREVIOUSYEAR_START AND PREVIOUSYEAR_END)
  AND REGEXP_CONTAINS(hits.page.pagePath, '/plan-compare/')
)
, plan_compare_agg AS (
  SELECT year
  ,COUNT(DISTINCT fullVisitorId) AS users
  ,COUNT(DISTINCT sessionId) AS sessions
  ,SUM(pageview) AS pageviews
  FROM plan_compare
  GROUP BY year
)
-- enroll data
,etl_enroll AS (SELECT EXTRACT(YEAR FROM date) AS year
  ,SUM(total_enrollments - csr_enrollments) AS web_enrollments
  ,SUM(csr_enrollments) AS csr_enrollments
  ,SUM(total_enrollments) AS total_enrollments
  FROM `steady-cat-772.etl_medicare_mct_enrollment.downloads_without_year`
  WHERE (date BETWEEN PARSE_DATE('%Y%m%d', CURRENTYEAR_START) AND PARSE_DATE('%Y%m%d', CURRENTYEAR_END)
    OR date BETWEEN PARSE_DATE('%Y%m%d', PREVIOUSYEAR_START) AND PARSE_DATE('%Y%m%d', PREVIOUSYEAR_END))
  GROUP BY year
)
, accounts AS (
  SELECT EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', date)) AS Year
  ,SUM(CAST(REGEXP_REPLACE(NewAccounts, ',', '') AS FLOAT64)) AS NewAccounts
  ,SUM(CAST(REGEXP_REPLACE(SuccessfulLogins, ',', '') AS FLOAT64)) AS SuccessfulLogins
  FROM `steady-cat-772.CMSGoogleSheets.MedicareAccountsTable`
  WHERE (PARSE_DATE('%Y-%m-%d', date) BETWEEN PARSE_DATE('%Y%m%d', CURRENTYEAR_START) AND PARSE_DATE('%Y%m%d', CURRENTYEAR_END)
    OR PARSE_DATE('%Y-%m-%d', date) BETWEEN PARSE_DATE('%Y%m%d', PREVIOUSYEAR_START) AND PARSE_DATE('%Y%m%d', PREVIOUSYEAR_END))
  GROUP BY Year
)
, temp AS (
  SELECT pc.year AS Year
  ,users AS `PlanFinder Users`
  ,sessions AS `PlanFinder Sessions`
  ,pageviews AS `PlanFinder Pageviews`
  ,web_enrollments AS `Online Enrollments`
  ,csr_enrollments AS `Call Center Enrollments`
  ,total_enrollments AS `Total Enrollments`
  ,CAST(NewAccounts AS INT64) AS `New Accounts`
  ,CAST(SuccessfulLogins AS INT64) AS `Successful Logins`
  FROM plan_compare_agg AS pc
  LEFT JOIN etl_enroll ON etl_enroll.year = pc.year
  LEFT JOIN accounts ON accounts.year = pc.year
)
-- SELECT * FROM temp
,t_current AS (SELECT *
FROM temp
UNPIVOT(values_current FOR metric IN (`PlanFinder Users`, `PlanFinder Sessions`, `PlanFinder Pageviews`, `Online Enrollments`, `Call Center Enrollments`, `Total Enrollments`, `New Accounts`, `Successful Logins`))
WHERE year = CAST(SUBSTR(CURRENTYEAR_START, 1, 4) AS INT64)
)
,t_previous AS (SELECT *
FROM temp
UNPIVOT(values_previous FOR metric IN (`PlanFinder Users`, `PlanFinder Sessions`, `PlanFinder Pageviews`, `Online Enrollments`, `Call Center Enrollments`, `Total Enrollments`, `New Accounts`, `Successful Logins`))
WHERE year = CAST(SUBSTR(PREVIOUSYEAR_START, 1, 4) AS INT64)
)
SELECT t_current.metric, FORMAT("%'d", SUM(values_current)) AS values_current, FORMAT("%'d", SUM(values_previous)) AS values_previous,
CONCAT(ROUND((SUM(values_current) - SUM(values_previous)) / SUM(values_previous) * 100), '%') AS YoY_Change
FROM t_current
LEFT JOIN t_previous ON t_previous.metric = t_current.metric
GROUP BY metric
                                    ;;
  }

  measure: count {
    type: count
  }



  dimension: metric {
    type: string
    sql: ${TABLE}.metric ;;
    alpha_sort: yes
  }

  dimension: OE_to_Date_Total{
    type: string
    sql: ${TABLE}.values_2021 ;;
  }


  dimension: 2020_OE_to_Date_Total {
    type: string
    sql: ${TABLE}.values_2020 ;;
  }

  dimension: perc_change_yoy {
    type: string
    sql: ${TABLE}.YoY_Change ;;
  }


  dimension: order {
    type: number
    sql:  CASE WHEN ${metric} = 'PlanFinder Sessions' THEN 1
              WHEN ${metric} = 'PlanFinder Users' THEN 2
              WHEN ${metric} = 'PlanFinder Pageviews' THEN 3
              WHEN ${metric} = 'Online Enrollments' THEN 4
              WHEN ${metric} = 'Call Center Enrollments' THEN 5
              WHEN ${metric} = 'Total Enrollments' THEN 6
              WHEN ${metric} = 'New Accounts' THEN 7
              WHEN ${metric} = 'Successful Logins' THEN 8
          END
    ;;
  }


}
