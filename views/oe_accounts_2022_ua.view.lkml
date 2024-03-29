view: oe_accounts_2022_ua {
  derived_table: {
    sql: --Medicare accounts
WITH sessions AS (
  SELECT EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', date)) AS Week
  ,date
  ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', date)) AS Year
  ,fullVisitorId
  ,concat(fullVisitorId, visitId, date) AS sessionId
  ,COUNTIF(hits.type = 'PAGE') AS pageviews
  FROM `steady-cat-772.30876903.ga_sessions_*`
  ,UNNEST(hits) AS hits
  WHERE (_TABLE_SUFFIX BETWEEN '20220604' AND '20220721' OR _TABLE_SUFFIX BETWEEN '20210604' AND '20210721')
  AND (REGEXP_CONTAINS(hits.page.pagePath, '/mbp/') OR REGEXP_CONTAINS(hits.page.pagePath, '/account/'))
  GROUP BY Week, Year, fullVisitorId, visitId, date
)

, accounts AS (
  SELECT EXTRACT(WEEK FROM PARSE_DATE('%Y-%m-%d', date)) AS Week
  ,EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', date)) AS Year
  ,SUM(CAST(REGEXP_REPLACE(NewAccounts, ',', '') AS FLOAT64)) AS `New Accounts`
  ,SUM(CAST(REGEXP_REPLACE(SuccessfulLogins, ',', '') AS FLOAT64)) AS `Successful Logins`
  ,ROUND(SUM(CAST(REGEXP_REPLACE(SuccessfulLogins, ',', '') AS FLOAT64)) /
  (SUM(CAST(REGEXP_REPLACE(SuccessfulLogins, ',', '') AS FLOAT64)) + SUM(CAST(REGEXP_REPLACE(FailedLogins, ',', '') AS FLOAT64))) * 100) AS `% Login Success`
  FROM `steady-cat-772.CMSGoogleSheets.MedicareAccountsTable`
  WHERE (date BETWEEN '2022-06-04' AND '2022-07-21' OR date BETWEEN '2021-06-04' AND '2021-07-21')
  GROUP BY Week, Year
  )

, temp_current AS (SELECT Week
  ,Year
  ,CONCAT(MIN(PARSE_DATE('%Y%m%d', date)), ' - ', MAX(PARSE_DATE('%Y%m%d', date))) AS date_range
  ,CAST(COUNT(DISTINCT fullVisitorId) AS FLOAT64) AS `Users`
  ,CAST(COUNT(DISTINCT sessionId) AS FLOAT64) AS `Sessions`
  ,CAST(SUM(pageviews) AS FLOAT64) AS `Pageviews`
  FROM sessions
  WHERE Year = 2022
  GROUP BY Week, Year
  )

, temp_previous AS (SELECT Week
  ,Year
  ,CONCAT(MIN(PARSE_DATE('%Y%m%d', date)), ' - ', MAX(PARSE_DATE('%Y%m%d', date))) AS date_range
  ,CAST(COUNT(DISTINCT fullVisitorId) AS FLOAT64) AS `Users`
  ,CAST(COUNT(DISTINCT sessionId) AS FLOAT64) AS `Sessions`
  ,CAST(SUM(pageviews) AS FLOAT64) AS `Pageviews`
  FROM sessions
  WHERE Year = 2021
  GROUP BY Week, Year
  )

, agg_current AS (SELECT temp_current.*, accounts.`New Accounts`, accounts.`Successful Logins`, accounts.`% Login Success`
  FROM temp_current
  LEFT JOIN accounts ON accounts.Week = temp_current.Week AND accounts.Year = temp_current.Year
  )

, agg_previous AS (SELECT temp_previous.*, accounts.`New Accounts`, accounts.`Successful Logins`, accounts.`% Login Success`
  FROM temp_previous
  LEFT JOIN accounts ON accounts.Week = temp_previous.Week AND accounts.Year = temp_previous.Year
  )
,t_current AS (SELECT *
  FROM agg_current
  UNPIVOT(values_current FOR metric IN (`Users`, `Sessions`, `Pageviews`, `New Accounts`, `Successful Logins`, `% Login Success`))
  )

,t_previous AS (SELECT *
  FROM agg_previous
  UNPIVOT(values_previous FOR metric IN (`Users`, `Sessions`, `Pageviews`, `New Accounts`, `Successful Logins`, `% Login Success`))
  )

SELECT CONCAT('Week ', t_current.Week - 21) AS Week, t_current.date_range, t_current.metric
,CASE WHEN t_current.metric IN ('Users', 'Sessions', 'Pageviews', 'New Accounts', 'Successful Logins') THEN CONCAT(FORMAT("%'d", CAST(values_current AS int64)))
WHEN t_current.metric = 'Sessions per User' THEN CONCAT(FORMAT("%'d", CAST(values_current AS int64)), SUBSTR(FORMAT("%.2f", CAST(values_current AS float64)), -3))
ELSE CONCAT(values_current, '%') END as values_current
,ROUND((values_current - LAG(values_current, 1, NULL) OVER (PARTITION BY t_current.metric ORDER BY t_current.Week)) /
LAG(values_current, 1, NULL) OVER (PARTITION BY t_current.metric ORDER BY t_current.Week), 2) AS prev_week
,CASE WHEN t_current.metric IN ('Users', 'Sessions', 'Pageviews', 'New Accounts', 'Successful Logins') THEN CONCAT(FORMAT("%'d", CAST(values_previous AS int64)))
WHEN t_current.metric = 'Sessions per User' THEN CONCAT(FORMAT("%'d", CAST(values_previous AS int64)), SUBSTR(FORMAT("%.2f", CAST(values_previous AS float64)), -3))
ELSE CONCAT(values_previous, '%') END as values_previous
,ROUND(SAFE_DIVIDE(values_current - values_previous, values_previous), 2) AS Perc_Change_YoY
FROM t_current LEFT JOIN t_previous ON t_previous.Week = t_current.Week AND t_previous.metric = t_current.metric
ORDER BY Week, CASE metric
WHEN 'Users' THEN 1
WHEN 'Sessions' THEN 2
WHEN 'Pageviews' THEN 3
WHEN 'New Accounts' THEN 4
WHEN 'Successful Logins' THEN 5
WHEN '% Login Success' THEN 6
END
      ;;
  }

  measure: count {
    type: count
  }

  dimension: week_of_year {
    type: string
    sql: ${TABLE}.Week ;;
    html:
          {% if value == 'Week 2' %}
          <p style="color: black; background-color: gainsboro; font-size:100%; text-align:center">{{ rendered_value }}</p>
          {% elsif value == 'Week 4' %}
          <p style="color: black; background-color: gainsboro; font-size:100%; text-align:center">{{ rendered_value }}</p>
          {% elsif value == 'Week 6' %}
          <p style="color: black; background-color: gainsboro; font-size:100%; text-align:center">{{ rendered_value }}</p>
          {% elsif value == 'Week 8' %}
          <p style="color: black; background-color: gainsboro; font-size:100%; text-align:center">{{ rendered_value }}</p>
          {% elsif value == 'Week 10' %}
          <p style="color: black; background-color: gainsboro; font-size:100%; text-align:center">{{ rendered_value }}</p>
          {% else %}
          <p style="color: black; background-color: white; font-size:100%; text-align:center">{{ rendered_value }}</p>
          {% endif %}
          ;;
  }

  dimension: date_range {
    type: string
    sql: ${TABLE}.date_range ;;
  }

  dimension: metric {
    type: string
    sql: ${TABLE}.metric ;;
    alpha_sort: yes
  }

  dimension: Current_Year_Weekly_Totals{
    type: string
    sql: ${TABLE}.values_current ;;
  }

  measure: previous_week {
    type: sum
    value_format: "0%"
    sql: ${TABLE}.prev_week ;;
  }

  dimension: Previous_Year_Weekly_Totals {
    type: string
    sql: ${TABLE}.values_previous ;;
  }

  measure: perc_change_yoy {
    type: sum
    value_format: "0%"
    sql: ${TABLE}.Perc_Change_YoY ;;
  }


  dimension: order {
    type: number
    sql:  CASE WHEN ${metric} = 'Sessions' THEN 1
           WHEN ${metric} = 'Users' THEN 2
           WHEN ${metric} = 'Pageviews' THEN 3
          WHEN ${metric} = 'New Accounts' THEN 4
           WHEN ${metric} = 'Successful Logins' THEN 5
          WHEN ${metric} = '% Login Success' THEN 6
          END
    ;;
  }

  dimension_group: date {
    type: time
    timeframes: [
      raw,
      date,
      week,
      month,
      quarter,
      year
    ]
    datatype: date
    sql: PARSE_DATE('%Y-%m-%d',LEFT(${TABLE}.date_range,10)) ;;
  }


}
