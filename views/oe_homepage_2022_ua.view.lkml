view: oe_homepage_2022_ua {
  derived_table: {
    sql: --homepage
    DECLARE CURRENTYEAR_START STRING DEFAULT '20211015';
DECLARE CURRENTYEAR_END STRING DEFAULT '20211207';
DECLARE PREVIOUSYEAR_START STRING DEFAULT '20201015';
DECLARE PREVIOUSYEAR_END STRING DEFAULT '20201207';

WITH sessions AS (SELECT EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', date)) AS Week
  ,date
  ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', date)) AS Year
  ,fullVisitorId
  ,concat(fullVisitorId, visitId, date) AS sessionId
  ,COUNTIF(hits.type = 'PAGE') AS pageviews
  ,CASE WHEN COUNTIF(device.deviceCategory = 'mobile' OR device.deviceCategory = 'tablet') > 0 THEN 1 ELSE 0 END AS mobile_user
  ,MAX(CASE WHEN totals.bounces = 1 THEN 1 ELSE 0 END) AS is_bounce
  ,MAX(case when hits.isEntrance = TRUE THEN 1 ELSE 0 END) AS is_entrance
  FROM `steady-cat-772.30876903.ga_sessions_*`
  ,UNNEST(hits) AS hits
  WHERE (_TABLE_SUFFIX BETWEEN CURRENTYEAR_START AND CURRENTYEAR_END OR _TABLE_SUFFIX BETWEEN PREVIOUSYEAR_START AND PREVIOUSYEAR_END)
  AND REGEXP_CONTAINS(hits.page.pagePath, '^/(\\?|$)')
  GROUP BY Week, Year, fullVisitorId, visitId, date
  )

, agg AS (SELECT Week
  ,Year
  ,CONCAT(MIN(PARSE_DATE('%Y%m%d', date)), ' - ', MAX(PARSE_DATE('%Y%m%d', date))) AS date_range
  ,CAST(COUNT(DISTINCT fullVisitorId) AS FLOAT64) AS `Users`
  ,CAST(COUNT(DISTINCT sessionId) AS FLOAT64) AS `Sessions`
  ,CAST(SUM(pageviews) AS FLOAT64) AS `Pageviews`
  ,ROUND(SUM(is_bounce) / SUM(is_entrance) * 100) AS `Bounce Rate`
  ,ROUND(AVG(mobile_user) * 100) AS `% Mobile Users`
  FROM sessions
  GROUP BY Week, Year)

,t_2021 AS (SELECT *
  FROM agg
  UNPIVOT(values_2021 FOR metric IN (`Users`, `Sessions`, `Pageviews`, `Bounce Rate`, `% Mobile Users`))
  WHERE year = 2021
  )

,t_2020 AS (SELECT *
  FROM agg
  UNPIVOT(values_2020 FOR metric IN (`Users`, `Sessions`, `Pageviews`, `Bounce Rate`, `% Mobile Users`))
  WHERE year = 2020
  )

SELECT CONCAT('Week ', t_2021.Week - 40) AS Week, t_2021.date_range, t_2021.metric
,CASE WHEN t_2021.metric IN ('Users', 'Sessions', 'Pageviews') THEN CONCAT(FORMAT("%'d", CAST(values_2021 AS int64)))
        WHEN t_2021.metric = 'Sessions per User' THEN CAST(ROUND(values_2021, 2) AS STRING)
        ELSE CONCAT(values_2021, '%') END as values_2021
,CONCAT(ROUND((values_2021 - LAG(values_2021, 1, NULL) OVER (PARTITION BY t_2021.metric ORDER BY t_2021.Week)) /
        LAG(values_2021, 1, NULL) OVER (PARTITION BY t_2021.metric ORDER BY t_2021.Week) * 100), '%') AS prev_week
,CASE WHEN t_2021.metric IN ('Users', 'Sessions', 'Pageviews') THEN CONCAT(FORMAT("%'d", CAST(values_2020 AS int64)))
        WHEN t_2021.metric = 'Sessions per User' THEN CAST(ROUND(values_2020, 2) AS STRING)
        ELSE CONCAT(values_2020, '%') END as values_2020
    ,CONCAT(ROUND(SAFE_DIVIDE(values_2021 - values_2020, values_2020)*100), '%') AS Perc_Change_YoY
FROM t_2021 LEFT JOIN t_2020 ON t_2020.Week = t_2021.Week AND t_2020.metric = t_2021.metric
ORDER BY Week, CASE metric
      WHEN 'Users' THEN 1
      WHEN 'Sessions' THEN 2
      WHEN 'Pageviews' THEN 3
      WHEN 'Bounce Rate' THEN 4
      WHEN 'Sessions per User' THEN 5
      WHEN '% Mobile Users' THEN 6
    END
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

  dimension: Current_Weekly_Totals{
    type: string
    sql: ${TABLE}.values_current ;;
  }

  dimension: previous_week {
    type: string
    sql: ${TABLE}.prev_week ;;
  }

  dimension: Previous_Weekly_Totals {
    type: string
    sql: ${TABLE}.values_previous ;;
  }

  dimension: perc_change_yoy {
    type: string
    sql: ${TABLE}.Perc_Change_YoY ;;
  }


  dimension: order {
    type: number
    sql:  CASE WHEN ${metric} = 'Sessions' THEN 1
           WHEN ${metric} = 'Users' THEN 2
           WHEN ${metric} = 'Pageviews' THEN 3
          WHEN ${metric} = 'Bounce Rate' THEN 4
           WHEN ${metric} = '% Mobile Users' THEN 5
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
