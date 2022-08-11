view: oe_homepage_2022_ua {
  derived_table: {
    sql: --homepage
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
  WHERE (_TABLE_SUFFIX BETWEEN '20220604' AND '20220721' OR _TABLE_SUFFIX BETWEEN '20210604' AND '20210721')
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

,t_current AS (SELECT *
  FROM agg
  UNPIVOT(values_current FOR metric IN (`Users`, `Sessions`, `Pageviews`, `Bounce Rate`, `% Mobile Users`))
  WHERE year = 2022
  )

,t_previous AS (SELECT *
  FROM agg
  UNPIVOT(values_previous FOR metric IN (`Users`, `Sessions`, `Pageviews`, `Bounce Rate`, `% Mobile Users`))
  WHERE year = 2021
  )

SELECT CONCAT('Week ', t_current.Week - 21) AS Week, t_current.date_range, t_current.metric
,CASE WHEN t_current.metric IN ('Users', 'Sessions', 'Pageviews') THEN CONCAT(FORMAT("%'d", CAST(values_current AS int64)))
        WHEN t_current.metric = 'Sessions per User' THEN CAST(ROUND(values_current, 2) AS STRING)
        ELSE CONCAT(values_current, '%') END as values_current
,CONCAT(ROUND((values_current - LAG(values_current, 1, NULL) OVER (PARTITION BY t_current.metric ORDER BY t_current.Week)) /
        LAG(values_current, 1, NULL) OVER (PARTITION BY t_current.metric ORDER BY t_current.Week) * 100), '%') AS prev_week
,CASE WHEN t_current.metric IN ('Users', 'Sessions', 'Pageviews') THEN CONCAT(FORMAT("%'d", CAST(values_previous AS int64)))
        WHEN t_current.metric = 'Sessions per User' THEN CAST(ROUND(values_previous, 2) AS STRING)
        ELSE CONCAT(values_previous, '%') END as values_previous
    ,CONCAT(ROUND(SAFE_DIVIDE(values_current - values_previous, values_previous)*100), '%') AS Perc_Change_YoY
FROM t_current LEFT JOIN t_previous ON t_previous.Week = t_current.Week AND t_previous.metric = t_current.metric
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

  dimension: previous_week {
    type: string
    sql: ${TABLE}.prev_week ;;
  }

  dimension: Previous_Year_Weekly_Totals {
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
