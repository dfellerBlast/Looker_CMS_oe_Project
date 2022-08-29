view: TEST {
  derived_table: {
    sql:--OE to date
      WITH plan_compare AS (
 SELECT fullVisitorId, visitId, CONCAT(fullVisitorId, visitId, date) AS sessionId, CAST(hits.type = 'PAGE' AS INT64) AS pageview
  ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', date)) AS year
  ,EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', date)) AS Week
  FROM `steady-cat-772.30876903.ga_sessions_*`
  ,UNNEST(hits) AS hits
  WHERE (_TABLE_SUFFIX BETWEEN '20220604' AND '20220721' OR _TABLE_SUFFIX BETWEEN '20210604' AND '20210721')
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
  WHERE (date BETWEEN '2022-06-04' AND '2022-07-21' OR date BETWEEN '2021-06-04' AND '2021-07-21')
  GROUP BY year
)
, accounts AS (
  SELECT EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', date)) AS Year
  ,SUM(CAST(REGEXP_REPLACE(NewAccounts, ',', '') AS FLOAT64)) AS NewAccounts
  ,SUM(CAST(REGEXP_REPLACE(SuccessfulLogins, ',', '') AS FLOAT64)) AS SuccessfulLogins
  FROM `steady-cat-772.CMSGoogleSheets.MedicareAccountsTable`
  WHERE (date BETWEEN '2022-06-04' AND '2022-07-21' OR date BETWEEN '2021-06-04' AND '2021-07-21')
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
WHERE year = 2022
)
,t_previous AS (SELECT *
FROM temp
UNPIVOT(values_previous FOR metric IN (`PlanFinder Users`, `PlanFinder Sessions`, `PlanFinder Pageviews`, `Online Enrollments`, `Call Center Enrollments`, `Total Enrollments`, `New Accounts`, `Successful Logins`))
WHERE year = 2021
)
SELECT t_current.metric,
FORMAT("%'d", SUM(values_current)) AS values_current
,FORMAT("%'d", SUM(values_previous)) AS values_previous
,ROUND((SUM(values_current) - SUM(values_previous)) / SUM(values_previous), 2) AS YoY_Change
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

  measure: Current_OE_to_Date_Total{
    type: number
    sql: ${TABLE}.values_current ;;
  }


  dimension: Previous_OE_to_Date_Total {
    type: string
    sql: ${TABLE}.values_previous ;;
  }

  measure: perc_change_yoy {
    type: sum
    value_format: "0\%"
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
