view: oe2022_weekly_medicaresitewide {
  derived_table: {
    sql: -- GA4 sitewide
WITH sessions AS (SELECT EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', event_date)) AS week_of_year
      ,event_date
      ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', event_date)) AS year
      ,user_pseudo_id
      ,CONCAT(user_pseudo_id, cast((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as string)) AS sessionId
      ,COUNTIF(event_name='page_view') AS pageviews
      ,CASE WHEN COUNTIF(device.category = 'mobile' OR device.category = 'tablet') > 0 THEN 1 ELSE 0 END AS mobile_user
      FROM `steady-cat-772.analytics_266429760.events_*`
      WHERE (_TABLE_SUFFIX BETWEEN '20211015' AND '20211207' OR _TABLE_SUFFIX BETWEEN '20201201' AND '20201201')
      AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'content_language') IS NOT NULL
      GROUP BY week_of_year, year, user_pseudo_id, sessionId, event_date
      )
,bounces AS (
    SELECT CONCAT(user_pseudo_id, cast((SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') as string)) AS sessionId
    ,1 AS is_bounce
    FROM `steady-cat-772.analytics_266429760.events_*`
    WHERE (_TABLE_SUFFIX BETWEEN '20211015' AND '20211207' OR _TABLE_SUFFIX BETWEEN '20201201' AND '20201201')
    GROUP BY sessionId
    HAVING MAX((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged')) ='0'
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
          WHERE (DATETIME_SUB(end_date, INTERVAL 4 HOUR) BETWEEN '2021-10-15' AND '2021-12-08') OR (DATETIME_SUB(end_date, INTERVAL 4 HOUR) BETWEEN '2020-10-15' AND '2020-12-08')
          GROUP BY week_of_year, year
      )

      , session_agg AS (SELECT s.week_of_year
      ,CONCAT(PARSE_DATE('%Y%m%d', MIN(event_date)), ' - ', PARSE_DATE('%Y%m%d', MAX(event_date))) AS date_range
      ,s.year
      ,COUNT(DISTINCT user_pseudo_id) AS users
      ,COUNT(DISTINCT s.sessionId) AS sessions
      ,SUM(pageviews) AS pageviews
      ,SUM(is_bounce)/COUNT(DISTINCT s.sessionId) AS bounce_rate
      ,AVG(mobile_user) AS mobile_users
      FROM sessions s
      LEFT JOIN bounces ON bounces.sessionId = s.sessionId
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
      ,t_2021 AS (
          SELECT * FROM temp
          UNPIVOT(values_2021 FOR metric IN (Users, Sessions, Pageviews, `Bounce Rate`, `% Mobile Users`, `Overall CSAT`, `Goal Completion %`
          ,`Surveys Completed`, `Beneficiary %`, `CoA %`, `Caregiver %`, `Professional %`))
          WHERE year = 2021
      )
      ,t_2020 AS (
          SELECT * FROM temp
          UNPIVOT(values_2020 FOR metric IN (Users, Sessions, Pageviews, `Bounce Rate`, `% Mobile Users`, `Overall CSAT`, `Goal Completion %`
          ,`Surveys Completed`, `Beneficiary %`, `CoA %`, `Caregiver %`, `Professional %`))
          WHERE year = 2020
      )
      SELECT CONCAT('Week ', t_2021.Week-40) AS Week, t_2021.Date_Range, t_2021.metric
          ,CASE WHEN t_2021.metric IN ('Users', 'Sessions', 'Pageviews', 'Surveys Completed') THEN CONCAT(FORMAT("%'d", CAST(values_2021 AS int64)))
              ELSE CONCAT(values_2021, '%') END as values_2021
          ,CONCAT(ROUND((values_2021 - LAG(values_2021, 1, NULL) OVER (PARTITION BY t_2021.metric ORDER BY t_2021.Week)) /
              LAG(values_2021, 1, NULL) OVER (PARTITION BY t_2021.metric ORDER BY t_2021.Week) * 100), '%') AS prev_week
          ,CASE WHEN t_2021.metric IN ('Users', 'Sessions', 'Pageviews', 'Surveys Completed')
              THEN CONCAT(FORMAT("%'d", CAST(values_2020 AS int64)))
              ELSE CONCAT(values_2020, '%') END as values_2020
          ,CONCAT(ROUND(SAFE_DIVIDE(values_2021 - values_2020, values_2020)*100), '%') AS Perc_Change_YoY
      FROM t_2021
      LEFT JOIN t_2020 ON t_2020.Week = t_2021.Week AND t_2020.metric = t_2021.metric
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

  dimension: weekly_totals{
    type: string
    sql: ${TABLE}.values_2021 ;;
  }

  dimension: previous_week {
    type: string
    sql: ${TABLE}.prev_week ;;
  }

  dimension: 2020_Weekly_Totals {
    type: string
    sql: ${TABLE}.values_2020 ;;
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
