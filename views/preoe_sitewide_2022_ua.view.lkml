view: preoe_sitewide_2022_ua {
  derived_table: {
    sql: --medicare sitewide oe
      WITH sessions AS (SELECT EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', ga.date)) AS week_of_year
        ,date
        ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', ga.date)) AS year
        ,ga.fullVisitorId
        ,concat(ga.fullVisitorId, ga.visitId, ga.date) AS sessionId
        ,COUNTIF(hits.type = 'PAGE') AS pageviews
        ,CASE WHEN COUNTIF(device.deviceCategory = 'mobile' OR device.deviceCategory = 'tablet') > 0 THEN 1 ELSE 0 END AS mobile_user
        ,MAX(CASE WHEN totals.bounces = 1 THEN 1 ELSE 0 END) AS is_bounce
        FROM `steady-cat-772.30876903.ga_sessions_*` AS ga
        ,UNNEST(hits) AS hits
        WHERE (_TABLE_SUFFIX BETWEEN '20220604' AND '20220721' OR _TABLE_SUFFIX BETWEEN '20210604' AND '20210721')
        GROUP BY week_of_year, year, ga.fullVisitorId, ga.visitId, ga.date
        )
      ,qualtrics AS (
        SELECT EXTRACT(WEEK FROM DATETIME_SUB(end_date, INTERVAL 4 HOUR)) AS week_of_year
        ,EXTRACT(YEAR FROM DATETIME_SUB(end_date, INTERVAL 4 HOUR)) AS year
        ,COUNTIF(q19a_a IN ('4', '5') OR q19a_b IN ('4', '5')) / (COUNT(q19a_a) + COUNT(q19a_b)) AS overall_csat
        ,COUNTIF(q14 = '1') / COUNT(q14) AS goal_completion_percent
        ,COUNT(DISTINCT response_id) AS surveys_completed
        ,COUNTIF(audience = 'Beneficiary') / COUNT(audience) AS bene_percent
        ,COUNTIF(audience = 'Coming of Ager') / COUNT(audience) AS coa_percent
        ,COUNTIF(audience = 'Caregiver') / COUNT(audience) AS caregiver_percent
        ,COUNTIF(audience = 'Professional') / COUNT(audience) AS professional_percent
        FROM `steady-cat-772.etl_medicare_qualtrics.site_wide_survey`
        WHERE (DATETIME_SUB(end_date, INTERVAL 4 HOUR) BETWEEN '2022-06-04' AND '2022-07-22' OR DATETIME_SUB(end_date, INTERVAL 4 HOUR) BETWEEN '2021-06-04' AND '2021-07-22')
        GROUP BY week_of_year, year
      )

      , session_agg AS (
      SELECT sessions.week_of_year
      ,CONCAT(PARSE_DATE('%Y%m%d', MIN(date)), ' - ', PARSE_DATE('%Y%m%d', MAX(date))) AS date_range
      ,sessions.year
      ,COUNT(DISTINCT fullVisitorId) AS users
      ,COUNT(DISTINCT sessionId) AS sessions
      ,SUM(pageviews) AS pageviews
      ,AVG(is_bounce) AS bounce_rate
      ,AVG(mobile_user) AS mobile_users
      FROM sessions
      GROUP BY week_of_year, year)

      ,temp AS (
      SELECT session_agg.week_of_year AS Week
      ,session_agg.year AS Year
      ,date_range AS Date_Range
      ,CAST(users AS FLOAT64) AS Users
      ,CAST(sessions AS FLOAT64) AS Sessions
      ,CAST(pageviews AS FLOAT64) AS Pageviews
      ,ROUND(bounce_rate * 100) AS `Bounce Rate`
      ,ROUND(mobile_users * 100) AS `% Mobile Users`
      ,ROUND(overall_csat * 100) AS `Overall CSAT`
      ,ROUND(goal_completion_percent * 100) AS `Goal Completion %`
      ,CAST(surveys_completed AS FLOAT64) AS `Surveys Completed`
      ,ROUND(bene_percent * 100) AS `Beneficiary %`
      ,ROUND(coa_percent * 100) AS `CoA %`
      ,ROUND(caregiver_percent * 100) AS `Caregiver %`
      ,ROUND(professional_percent * 100) AS `Professional %`
      FROM session_agg
      LEFT JOIN qualtrics ON qualtrics.week_of_year = session_agg.week_of_year AND qualtrics.year = session_agg.year
      )
      ,t_current AS (
      SELECT * FROM temp
      UNPIVOT(values_current FOR metric IN (Users, Sessions, Pageviews, `Bounce Rate`, `% Mobile Users`, `Overall CSAT`, `Goal Completion %`
      ,`Surveys Completed`, `Beneficiary %`, `CoA %`, `Caregiver %`, `Professional %`))
      WHERE year = 2022
      )
      ,t_previous AS (
      SELECT * FROM temp
      UNPIVOT(values_previous FOR metric IN (Users, Sessions, Pageviews, `Bounce Rate`, `% Mobile Users`, `Overall CSAT`, `Goal Completion %`
      ,`Surveys Completed`, `Beneficiary %`, `CoA %`, `Caregiver %`, `Professional %`))
      WHERE year = 2021
      )
      SELECT CONCAT('Week ', t_current.Week-21) AS Week, t_current.Date_Range, t_current.metric
      ,CASE WHEN t_current.metric IN ('Users', 'Sessions', 'Pageviews', 'Surveys Completed') THEN CONCAT(FORMAT("%'d", CAST(values_current AS int64)))
      ELSE CONCAT(values_current, '%') END as values_current
      ,CONCAT(ROUND(SAFE_DIVIDE((values_current - LAG(values_current, 1, NULL) OVER (PARTITION BY t_current.metric ORDER BY t_current.Week)),
      LAG(values_current, 1, NULL) OVER (PARTITION BY t_current.metric ORDER BY t_current.Week)) * 100), '%') AS prev_week
      ,CASE WHEN t_current.metric IN ('Users', 'Sessions', 'Pageviews', 'Surveys Completed')
      THEN CONCAT(FORMAT("%'d", CAST(values_previous AS int64)))
      ELSE CONCAT(values_previous, '%') END as values_previous
      ,CONCAT(ROUND(SAFE_DIVIDE(values_current - values_previous, values_previous)*100), '%') AS Perc_Change_YoY
      FROM t_current
      LEFT JOIN t_previous ON t_previous.Week = t_current.Week AND t_previous.metric = t_current.metric
      ORDER BY Week, CASE metric
      WHEN 'Users' THEN 1
      WHEN 'Sessions' THEN 2
      WHEN 'Pageviews' THEN 3
      WHEN 'Bounce Rate' THEN 4
      WHEN '% Mobile Users' THEN 5
      WHEN 'Overall CSAT' THEN 6
      WHEN 'Goal Completion %' THEN 7
      WHEN 'Surveys Completed' THEN 8
      WHEN 'Beneficiary %' THEN 9
      WHEN 'CoA %' THEN 10
      WHEN 'Caregiver %' THEN 11
      WHEN 'Professional %' THEN 12
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
    sql: ${TABLE}.Date_Range ;;
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
           WHEN ${metric} = 'Overall CSAT' THEN 6
           WHEN ${metric} = 'Goal Completion %' THEN 7
          WHEN ${metric} = 'Surveys Completed' THEN 8
           WHEN ${metric} = 'Beneficiary %' THEN 9
           WHEN ${metric} = 'CoA %' THEN 10
           WHEN ${metric} = 'Caregiver %' THEN 11
           WHEN ${metric} = 'Professional %' THEN 12
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
