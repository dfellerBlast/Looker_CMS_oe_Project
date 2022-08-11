view: oe_planfinder_2022_ua {
  derived_table: {
    sql:
    -- plan finder metrics
DECLARE CURRENTYEAR_START STRING DEFAULT '20211015';
DECLARE CURRENTYEAR_END STRING DEFAULT '20211207';
DECLARE PREVIOUSYEAR_START STRING DEFAULT '20201015';
DECLARE PREVIOUSYEAR_END STRING DEFAULT '20201207';

WITH plan_compare AS (
  SELECT DISTINCT fullVisitorId, visitId, CONCAT(fullVisitorId, visitId, date) AS sessionId
  ,EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', date)) AS week_of_year
  ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', date)) AS year
  ,COUNTIF(hits.type = 'PAGE') AS pageviews
  FROM `steady-cat-772.30876903.ga_sessions_*`
  ,UNNEST(hits) AS hits
  WHERE (_TABLE_SUFFIX BETWEEN CURRENTYEAR_START AND CURRENTYEAR_END OR _TABLE_SUFFIX BETWEEN PREVIOUSYEAR_START AND PREVIOUSYEAR_END)
  AND REGEXP_CONTAINS(hits.page.pagePath, '/plan-compare/')
  GROUP BY fullVisitorId, visitId, date, year, week_of_year
)
-- user level info
, user_data AS (
  SELECT ga.fullVisitorId
  ,EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', date)) AS week_of_year
  ,date
  ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', date)) AS year
  ,MAX(CASE WHEN totals.newVisits = 1 THEN 1 ELSE 0 END) AS is_new
  FROM `steady-cat-772.30876903.ga_sessions_*` AS ga
  WHERE (_TABLE_SUFFIX BETWEEN CURRENTYEAR_START AND CURRENTYEAR_END OR _TABLE_SUFFIX BETWEEN PREVIOUSYEAR_START AND PREVIOUSYEAR_END)
  AND CONCAT(fullVisitorId, visitId, date) IN (SELECT sessionId FROM plan_compare)
  GROUP BY week_of_year, year, ga.fullVisitorId, date
)

,user_agg_current AS (
  SELECT week_of_year
  ,CONCAT(PARSE_DATE('%Y%m%d', MIN(date)), ' - ', PARSE_DATE('%Y%m%d', MAX(date))) AS date_range
  ,year
  ,SUM(is_new) / COUNT(DISTINCT fullVisitorId) AS new_user_percent
  FROM user_data
  WHERE year = CAST(SUBSTR(CURRENTYEAR_START, 1, 4) AS INT64)
  GROUP BY week_of_year, year
)

,user_agg_previous AS (
  SELECT week_of_year
  ,CONCAT(PARSE_DATE('%Y%m%d', MIN(date)), ' - ', PARSE_DATE('%Y%m%d', MAX(date))) AS date_range
  ,year
  ,SUM(is_new) / COUNT(DISTINCT fullVisitorId) AS new_user_percent
  FROM user_data
  WHERE year = CAST(SUBSTR(PREVIOUSYEAR_START, 1, 4) AS INT64)
  GROUP BY week_of_year, year
)

,sessions AS (
  SELECT EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', ga.date)) AS week_of_year
  ,date
  ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', ga.date)) AS year
  ,ga.fullVisitorId
  ,concat(ga.fullVisitorId, ga.visitId, ga.date) AS sessionId
  ,CASE WHEN COUNTIF(device.deviceCategory = 'mobile' OR device.deviceCategory = 'tablet') > 0 THEN 1 ELSE 0 END AS mobile_user
  ,MAX(CASE WHEN totals.bounces = 1 THEN 1 ELSE 0 END) AS is_bounce
  ,CASE WHEN COUNTIF(REGEXP_CONTAINS(hits.page.pagePath, '\\/plan-compare\\/#\\/[a-zA-Z]')) > 0 THEN 1 ELSE 0 END AS interact
  ,CASE WHEN COUNTIF(REGEXP_CONTAINS(hits.eventinfo.eventLabel, 'MAPD')
  AND REGEXP_CONTAINS(hits.eventinfo.eventAction, 'Find Plans - Enter Your Information')
  AND REGEXP_CONTAINS(hits.eventinfo.eventCategory, 'MCT')) > 0 THEN 1 ELSE 0 END AS ma_session
  ,CASE WHEN COUNTIF(REGEXP_CONTAINS(hits.eventinfo.eventCategory, 'MCT') AND REGEXP_CONTAINS(hits.eventinfo.eventAction, 'Find Plans - Enter Your Information') AND
  (hits.eventinfo.eventLabel = 'Part D + Medigap' OR hits.eventinfo.eventLabel = 'Part D')) > 0 THEN 1 ELSE 0 END AS pdp_session
  ,CASE WHEN COUNTIF(hits.eventInfo.eventCategory = 'MCT' AND hits.eventInfo.eventAction = 'Find Plans Landing Page - Login' AND hits.eventInfo.eventLabel = 'Login') > 0 THEN 1 ELSE 0 END AS logged_in
  ,CASE WHEN COUNTIF(hits.eventInfo.eventCategory = 'MCT' AND hits.eventInfo.eventAction = 'Find Plans Landing Page - Login' AND REGEXP_CONTAINS(hits.eventInfo.eventLabel,'Continue without logging in')) > 0 THEN 1 ELSE 0 END AS anonymous
  ,CASE WHEN COUNTIF(REGEXP_CONTAINS(hits.eventinfo.eventCategory, 'MCT') AND REGEXP_CONTAINS(hits.eventinfo.eventAction, 'Find Plans') AND REGEXP_CONTAINS(hits.eventinfo.eventLabel, 'Enroll')) > 0 THEN 1 ELSE 0 END AS enrolled
  ,CASE WHEN COUNTIF(REGEXP_CONTAINS(hits.page.pagePath, '/plan-compare/#/search-results')) > 0 THEN 1 ELSE 0 END AS plan_results
  FROM `steady-cat-772.30876903.ga_sessions_*` AS ga
  ,UNNEST(hits) AS hits
  WHERE (_TABLE_SUFFIX BETWEEN CURRENTYEAR_START AND CURRENTYEAR_END OR _TABLE_SUFFIX BETWEEN PREVIOUSYEAR_START AND PREVIOUSYEAR_END)
  GROUP BY week_of_year, year, ga.fullVisitorId, ga.visitId, ga.date
)

,session_agg_current AS (
  SELECT sessions.week_of_year
  ,CONCAT(PARSE_DATE('%Y%m%d', MIN(date)), ' - ', PARSE_DATE('%Y%m%d', MAX(date))) AS date_range
  ,sessions.year
  ,COUNT(DISTINCT sessions.fullVisitorId) AS users
  ,COUNT(DISTINCT sessions.sessionId) AS sessions
  ,SUM(plan_compare.pageviews) AS pageviews
  ,AVG(is_bounce) AS bounce_rate
  ,COUNT(DISTINCT sessions.sessionId) / COUNT(DISTINCT sessions.fullVisitorId) AS sessions_per_user
  ,AVG(mobile_user) AS mobile_users
  ,COUNTIF(ma_session = 1 AND pdp_session = 0) / COUNTIF(ma_session = 1 OR pdp_session = 1) AS ma_percent
  ,COUNTIF(ma_session = 0 AND pdp_session = 1) / COUNTIF(ma_session = 1 OR pdp_session = 1) AS pdp_percent
  ,COUNTIF(ma_session = 1 AND pdp_session = 1) / COUNTIF(ma_session = 1 OR pdp_session = 1) AS both_percent
  ,SUM(logged_in) / (SUM(logged_in) + SUM(anonymous)) AS logged_in
  ,SUM(anonymous) / (SUM(logged_in) + SUM(anonymous)) AS anonymous
  ,SUM(enrolled) / COUNT(DISTINCT sessions.sessionId) AS enroll_allsession_perc
  ,SUM(enrolled) / COUNTIF(is_bounce = 0) AS enroll_nonbounce_perc
  ,SUM(plan_results) / COUNT(DISTINCT sessions.sessionId) AS plan_results_all_perc
  ,SUM(plan_results) / SUM(interact) AS plan_results_nonbounce_perc
  FROM sessions
  INNER JOIN plan_compare ON plan_compare.sessionId = sessions.sessionId AND plan_compare.year = sessions.year AND plan_compare.week_of_year = sessions.week_of_year
  WHERE sessions.year=CAST(SUBSTR(CURRENTYEAR_START, 1, 4) AS INT64)
  GROUP BY week_of_year, year
)

,session_agg_previous AS (
  SELECT sessions.week_of_year
  ,CONCAT(PARSE_DATE('%Y%m%d', MIN(date)), ' - ', PARSE_DATE('%Y%m%d', MAX(date))) AS date_range
  ,sessions.year
  ,COUNT(DISTINCT sessions.fullVisitorId) AS users
  ,COUNT(DISTINCT sessions.sessionId) AS sessions
  ,SUM(pageviews) AS pageviews
  ,AVG(is_bounce) AS bounce_rate
  ,COUNT(DISTINCT sessions.sessionId) / COUNT(DISTINCT sessions.fullVisitorId) AS sessions_per_user
  ,AVG(mobile_user) AS mobile_users
  ,COUNTIF(ma_session = 1 AND pdp_session = 0) / COUNTIF(ma_session = 1 OR pdp_session = 1) AS ma_percent
  ,COUNTIF(ma_session = 0 AND pdp_session = 1) / COUNTIF(ma_session = 1 OR pdp_session = 1) AS pdp_percent
  ,COUNTIF(ma_session = 1 AND pdp_session = 1) / COUNTIF(ma_session = 1 OR pdp_session = 1) AS both_percent
  ,SUM(logged_in) / (SUM(logged_in) + SUM(anonymous)) AS logged_in
  ,SUM(anonymous) / (SUM(logged_in) + SUM(anonymous)) AS anonymous
  ,SUM(enrolled) / COUNT(DISTINCT sessions.sessionId) AS enroll_allsession_perc
  ,SUM(enrolled) / COUNTIF(is_bounce = 0) AS enroll_nonbounce_perc
  ,SUM(plan_results) / COUNT(DISTINCT sessions.sessionId) AS plan_results_all_perc
  ,SUM(plan_results) / SUM(interact) AS plan_results_nonbounce_perc
  FROM sessions
  INNER JOIN plan_compare ON plan_compare.sessionId = sessions.sessionId AND plan_compare.year = sessions.year AND plan_compare.week_of_year = sessions.week_of_year
  WHERE sessions.year=CAST(SUBSTR(PREVIOUSYEAR_START, 1, 4) AS INT64)
  GROUP BY week_of_year, year
)

-- enroll data
,etl_enroll AS (
  SELECT EXTRACT(WEEK FROM date) AS week_of_year
  ,EXTRACT(YEAR FROM date) AS year
  ,SUM(total_enrollments - csr_enrollments) AS web_enrollments
  ,SUM(csr_enrollments) AS csr_enrollments
  ,SUM(total_enrollments) AS total_enrollments
  FROM `steady-cat-772.etl_medicare_mct_enrollment.downloads_with_year`
  WHERE (date BETWEEN PARSE_DATE('%Y%m%d', CURRENTYEAR_START) AND PARSE_DATE('%Y%m%d', CURRENTYEAR_END)
  OR date BETWEEN PARSE_DATE('%Y%m%d', PREVIOUSYEAR_START) AND PARSE_DATE('%Y%m%d', PREVIOUSYEAR_END))
  GROUP BY week_of_year, year
)

--qualtrics data
,qualtrics AS (
  SELECT EXTRACT(WEEK FROM DATETIME_SUB(end_date, INTERVAL 4 HOUR)) AS week_of_year
  ,EXTRACT(YEAR FROM DATETIME_SUB(end_date, INTERVAL 4 HOUR)) AS year
  ,COUNTIF(q19a_a IN ('4', '5') OR q19a_b IN ('4', '5')) / (COUNT(q19a_a) + COUNT(q19a_b)) AS overall_csat
  ,COUNTIF(q14 = '1') / COUNT(q14) AS goal_completion_percent
  ,COUNTIF(q18 = '3') / COUNT(q18) AS will_contact_cc
  FROM `steady-cat-772.etl_medicare_qualtrics.site_wide_survey`
  WHERE (REGEXP_CONTAINS(tools_use, 'MCT') OR REGEXP_CONTAINS(tools_use, 'Plan Finder'))
  AND (DATETIME_SUB(end_date, INTERVAL 4 HOUR) BETWEEN PARSE_DATE('%Y%m%d', CURRENTYEAR_START) AND PARSE_DATE('%Y%m%d', CURRENTYEAR_END) + 1 OR
    DATETIME_SUB(end_date, INTERVAL 4 HOUR) BETWEEN PARSE_DATE('%Y%m%d', PREVIOUSYEAR_START) AND PARSE_DATE('%Y%m%d', PREVIOUSYEAR_END) + 1)
  GROUP BY week_of_year, year
)

-- medigap and wizard tool tables
, medigap_wizard AS (
  SELECT EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', ga.date)) AS week_of_year
  ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', ga.date)) AS year
  ,ga.fullVisitorId
  ,concat(ga.fullVisitorId, ga.visitId, ga.date) AS sessionId
  ,CASE WHEN COUNTIF(REGEXP_CONTAINS(hits.page.pagePath, 'medicarecoverageoptions') OR REGEXP_CONTAINS(hits.page.pagePath, 'medicare-coverage-options') OR REGEXP_CONTAINS(hits.page.pagePath, 'plan-compare/#/coverage-options')) > 0 THEN 1 ELSE 0 END AS wizard_session
  ,CASE WHEN COUNTIF(REGEXP_CONTAINS(hits.eventinfo.eventCategory, 'MCT') AND (REGEXP_CONTAINS(hits.eventinfo.eventAction, 'Coverage Wizard - Plan Options') OR REGEXP_CONTAINS(hits.eventinfo.eventAction, 'Coverage Wizard - Options')) AND (REGEXP_CONTAINS(hits.eventinfo.eventLabel, 'Ready to Continue') OR REGEXP_CONTAINS(hits.eventinfo.eventLabel, 'Look at Plans'))) > 0 THEN 1 ELSE 0 END AS wizard_convert
  ,CASE WHEN COUNTIF(REGEXP_CONTAINS(hits.page.pagePath, '/medigap-supplemental-insurance-plans/#/m') OR REGEXP_CONTAINS(hits.page.pagePath, '/find-a-plan/.*/medigap')) > 0 THEN 1 ELSE 0 END AS medigap_session
  ,CASE WHEN COUNTIF(REGEXP_CONTAINS(hits.page.pagePath, 'medigap-supplemental-insurance-plans/results') OR REGEXP_CONTAINS(hits.page.pagePath, 'medigap-supplemental-insurance-plans/#/results') OR REGEXP_CONTAINS(hits.page.pagePath, 'medigap-supplemental-insurance-plans/#/m/plans')) > 0 THEN 1 ELSE 0 END AS medigap_convert
  FROM `steady-cat-772.30876903.ga_sessions_*` AS ga
  ,UNNEST(hits) AS hits
  WHERE (_TABLE_SUFFIX BETWEEN CURRENTYEAR_START AND CURRENTYEAR_END OR _TABLE_SUFFIX BETWEEN PREVIOUSYEAR_START AND PREVIOUSYEAR_END)
  GROUP BY week_of_year, year, ga.fullVisitorId, ga.visitId, date
)

, medigap_wizard_agg AS (
  SELECT week_of_year, year
  ,SUM(wizard_session) AS wizard_sessions
  ,SUM(wizard_convert) / SUM(wizard_session) AS wizard_convert_percent
  ,SUM(medigap_session) AS medigap_sessions
  ,SUM(medigap_convert) / SUM(medigap_session) AS medigap_convert_percent
  FROM medigap_wizard
  GROUP BY week_of_year, year
)

, full_table_current AS(
  SELECT CONCAT('Week ', user_agg_current.week_of_year-40) AS Week, user_agg_current.date_range
  ,CAST(sessions AS FLOAT64) AS Sessions, CAST(users AS FLOAT64) AS Users, CAST(pageviews AS FLOAT64) AS Pageviews
  ,ROUND(bounce_rate * 100) AS `Bounce Rate`
  ,ROUND(sessions_per_user, 2) AS `Sessions Per User`
  ,ROUND(new_user_percent * 100) AS `% New Users`, ROUND(mobile_users * 100) AS `% Mobile Users`
  ,ROUND(logged_in * 100) AS `Logged In %`, ROUND(anonymous * 100) AS `Anonymous %`
  ,ROUND(pdp_percent * 100) AS `PDP PlanType Clicks %`, ROUND(ma_percent * 100) AS `MA PlanType Clicks %`
  ,ROUND(both_percent * 100) AS `MA & PDP PlanType Clicks %`
  ,ROUND(enroll_allsession_perc * 100) AS `Enroll All Sessions %`, ROUND(enroll_nonbounce_perc * 100) AS `Enroll Non-Bounce %`
  ,ROUND(plan_results_all_perc * 100) AS `Plan Results All Sessions %`, ROUND(plan_results_nonbounce_perc * 100) AS `Plan Results Non-Bounce %`
  ,CAST(wizard_sessions AS FLOAT64) AS `Wizard Sessions`, ROUND(wizard_convert_percent * 100) AS `Wizard Conversions %`
  ,CAST(medigap_sessions AS FLOAT64) AS `Medigap Sessions`, ROUND(medigap_convert_percent * 100) AS `Medigap Conversion %`
  ,CAST(web_enrollments AS FLOAT64) AS `Online Enrollments`
  ,CAST(csr_enrollments AS FLOAT64) AS `Call Center Enrollments`, CAST(total_enrollments AS FLOAT64) AS `Total Enrollments`
  ,ROUND(overall_csat * 100) AS `Overall CSAT`
  ,ROUND(goal_completion_percent * 100) AS `Goal Completion %`
  ,ROUND(will_contact_cc * 100) AS `Will Contact Call Center`
  FROM user_agg_current
  LEFT JOIN session_agg_current ON session_agg_current.week_of_year = user_agg_current.week_of_year AND session_agg_current.year = user_agg_current.year
  LEFT JOIN etl_enroll ON etl_enroll.week_of_year = user_agg_current.week_of_year AND etl_enroll.year = user_agg_current.year
  LEFT JOIN qualtrics ON qualtrics.week_of_year = user_agg_current.week_of_year AND qualtrics.year = user_agg_current.year
  LEFT JOIN medigap_wizard_agg ON medigap_wizard_agg.week_of_year = user_agg_current.week_of_year AND medigap_wizard_agg.year = user_agg_current.year
  WHERE user_agg_current.year = CAST(SUBSTR(CURRENTYEAR_START, 1, 4) AS INT64)
  AND qualtrics.year = CAST(SUBSTR(CURRENTYEAR_START, 1, 4) AS INT64)
  AND medigap_wizard_agg.year = CAST(SUBSTR(CURRENTYEAR_START, 1, 4) AS INT64)
  AND etl_enroll.year = CAST(SUBSTR(CURRENTYEAR_START, 1, 4) AS INT64)
  ORDER BY Week
)

, full_table_previous AS(
  SELECT CONCAT('Week ', user_agg_previous.week_of_year-40) AS Week, user_agg_previous.date_range
  ,CAST(sessions AS FLOAT64) AS Sessions, CAST(users AS FLOAT64) AS Users, CAST(pageviews AS FLOAT64) AS Pageviews
  ,ROUND(bounce_rate * 100) AS `Bounce Rate`
  ,ROUND(sessions_per_user, 2) AS `Sessions Per User`
  ,ROUND(new_user_percent * 100) AS `% New Users`, ROUND(mobile_users * 100) AS `% Mobile Users`
  ,ROUND(logged_in * 100) AS `Logged In %`, ROUND(anonymous * 100) AS `Anonymous %`
  ,ROUND(pdp_percent * 100) AS `PDP PlanType Clicks %`, ROUND(ma_percent * 100) AS `MA PlanType Clicks %`
  ,ROUND(both_percent * 100) AS `MA & PDP PlanType Clicks %`
  ,ROUND(enroll_allsession_perc * 100) AS `Enroll All Sessions %`, ROUND(enroll_nonbounce_perc * 100) AS `Enroll Non-Bounce %`
  ,ROUND(plan_results_all_perc * 100) AS `Plan Results All Sessions %`, ROUND(plan_results_nonbounce_perc * 100) AS `Plan Results Non-Bounce %`
  ,CAST(wizard_sessions AS FLOAT64) AS `Wizard Sessions`, ROUND(wizard_convert_percent * 100) AS `Wizard Conversions %`
  ,CAST(medigap_sessions AS FLOAT64) AS `Medigap Sessions`, ROUND(medigap_convert_percent * 100) AS `Medigap Conversion %`
  ,CAST(web_enrollments AS FLOAT64) AS `Online Enrollments`
  ,CAST(csr_enrollments AS FLOAT64) AS `Call Center Enrollments`, CAST(total_enrollments AS FLOAT64) AS `Total Enrollments`
  ,ROUND(overall_csat * 100) AS `Overall CSAT`
  ,ROUND(goal_completion_percent * 100) AS `Goal Completion %`
  ,ROUND(will_contact_cc * 100) AS `Will Contact Call Center`
  FROM user_agg_previous
  LEFT JOIN session_agg_previous ON session_agg_previous.week_of_year = user_agg_previous.week_of_year
  LEFT JOIN etl_enroll ON etl_enroll.week_of_year = user_agg_previous.week_of_year AND etl_enroll.year = user_agg_previous.year
  LEFT JOIN qualtrics ON qualtrics.week_of_year = user_agg_previous.week_of_year AND qualtrics.year = user_agg_previous.year
  LEFT JOIN medigap_wizard_agg ON medigap_wizard_agg.week_of_year = user_agg_previous.week_of_year AND medigap_wizard_agg.year = user_agg_previous.year
  WHERE user_agg_previous.year = CAST(SUBSTR(PREVIOUSYEAR_START, 1, 4) AS INT64)
  AND etl_enroll.year = CAST(SUBSTR(PREVIOUSYEAR_START, 1, 4) AS INT64)
  AND qualtrics.year = CAST(SUBSTR(PREVIOUSYEAR_START, 1, 4) AS INT64)
  AND medigap_wizard_agg.year = CAST(SUBSTR(PREVIOUSYEAR_START, 1, 4) AS INT64)
  ORDER BY Week
)

,t_current AS (
  SELECT * FROM full_table_current
  UNPIVOT(values_current FOR metric IN (Sessions, Users, Pageviews, `Bounce Rate`, `Sessions Per User`, `% New Users`, `% Mobile Users`, `Logged In %`, `Anonymous %`, `PDP PlanType Clicks %`
  ,`MA PlanType Clicks %`, `MA & PDP PlanType Clicks %`, `Enroll All Sessions %`, `Enroll Non-Bounce %`, `Plan Results All Sessions %`
  ,`Plan Results Non-Bounce %`, `Wizard Sessions`, `Wizard Conversions %`, `Medigap Sessions`, `Medigap Conversion %`,`Online Enrollments`,`Call Center Enrollments`
  ,`Total Enrollments`, `Overall CSAT`,`Goal Completion %`, `Will Contact Call Center`))
)

,t_previous AS (
  SELECT * FROM full_table_previous
  UNPIVOT(values_previous FOR metric IN (Sessions, Users, Pageviews, `Bounce Rate`, `Sessions Per User`, `% New Users`, `% Mobile Users`, `Logged In %`, `Anonymous %`, `PDP PlanType Clicks %`
  ,`MA PlanType Clicks %`, `MA & PDP PlanType Clicks %`, `Enroll All Sessions %`, `Enroll Non-Bounce %`, `Plan Results All Sessions %`
  ,`Plan Results Non-Bounce %`, `Wizard Sessions`, `Wizard Conversions %`, `Medigap Sessions`, `Medigap Conversion %`,`Online Enrollments`,`Call Center Enrollments`
  ,`Total Enrollments`, `Overall CSAT`,`Goal Completion %`, `Will Contact Call Center`))
)

SELECT t_current.Week, t_current.date_range, t_current.metric
,CASE WHEN t_current.metric IN ('Sessions', 'Users', 'Pageviews', 'Wizard Sessions', 'Medigap Sessions', 'Online Enrollments', 'Call Center Enrollments', 'Total Enrollments')
THEN CONCAT(FORMAT("%'d", CAST(values_current AS int64)))
WHEN t_current.metric = 'Sessions Per User' THEN CAST(ROUND(values_current, 2) AS STRING)
ELSE CONCAT(values_current, '%') END as values_current
,CONCAT(ROUND(SAFE_DIVIDE(values_current - LAG(values_current, 1, NULL) OVER (PARTITION BY t_current.metric ORDER BY t_current.Week),
LAG(values_current, 1, NULL) OVER (PARTITION BY t_current.metric ORDER BY t_current.Week)) * 100), '%') AS prev_week
,CASE WHEN t_current.metric IN ('Sessions', 'Users', 'Pageviews', 'Wizard Sessions', 'Medigap Sessions', 'Online Enrollments', 'Call Center Enrollments', 'Total Enrollments')
THEN CONCAT(FORMAT("%'d", CAST(values_previous AS int64)))
WHEN t_current.metric = 'Sessions Per User' THEN CAST(ROUND(values_previous, 2) AS STRING)
ELSE CONCAT(values_previous, '%') END as values_previous
,CONCAT(ROUND(SAFE_DIVIDE(values_current - values_previous, values_previous)*100), '%') AS Perc_Change_YoY
FROM t_current
LEFT JOIN t_previous ON t_previous.Week = t_current.Week AND t_previous.metric = t_current.metric
ORDER BY Week, CASE metric
WHEN 'Sessions' THEN 1
WHEN 'Users' THEN 2
WHEN 'Pageviews' THEN 3
WHEN 'Bounce Rate' THEN 4
WHEN 'Sessions Per User' THEN 5
WHEN '% New Users' THEN 6
WHEN '% Mobile Users' THEN 7
WHEN 'Logged In %' THEN 8
WHEN 'Anonymous %' THEN 9
WHEN 'PDP PlanType Clicks %' THEN 10
WHEN 'MA PlanType Clicks %' THEN 11
WHEN 'MA & PDP PlanType Clicks %' THEN 12
WHEN 'Enroll All Sessions %' THEN 14
WHEN 'Enroll Non-Bounce %' THEN 15
WHEN 'Plan Results All Sessions %' THEN 16
WHEN 'Plan Results Non-Bounce %' THEN 17
WHEN 'Wizard Sessions' THEN 18
WHEN 'Wizard Conversions %' THEN 19
WHEN 'Medigap Sessions' THEN 20
WHEN 'Medigap Conversion %' THEN 21
WHEN 'Online Enrollments' THEN 22
WHEN 'Call Center Enrollments' THEN 23
WHEN 'Total Enrollments' THEN 24
WHEN 'Overall CSAT' THEN 25
WHEN 'Goal Completion %' THEN 26
WHEN 'Will Contact Call Center' THEN 27
END
      ;;
  }


  measure: count {
    type: count
    drill_fields: [detail*]
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

  dimension: weekly_totals {
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

  dimension: Perc_Change_YoY {
    type: string
    sql: ${TABLE}.Perc_Change_YoY ;;
  }

  dimension: order {
    type: number
    sql:  CASE WHEN ${metric} = 'Sessions' THEN 1
           WHEN ${metric} = 'Users' THEN 2
           WHEN ${metric} = 'Pageviews' THEN 3
          WHEN ${metric} = 'Bounce Rate' THEN 4
           WHEN ${metric} = 'Sessions Per User' THEN 5
           WHEN ${metric} = '% New Users' THEN 6
           WHEN ${metric} = '% Mobile Users' THEN 7
           WHEN ${metric} = 'Logged In %' THEN 8
           WHEN ${metric} = 'Anonymous %' THEN 9
          WHEN ${metric} = 'PDP PlanType Clicks %' THEN 10
           WHEN ${metric} = 'MA PlanType Clicks %' THEN 11
           WHEN ${metric} = 'MA & PDP PlanType Clicks %' THEN 12
           WHEN ${metric} = 'Insulin Demo Filter Clicks (Total)' THEN 13
           WHEN ${metric} = 'Enroll All Sessions %' THEN 14
           WHEN ${metric} = 'Enroll Non-Bounce %' THEN 15
           WHEN ${metric} = 'Plan Results All Sessions %' THEN 16
           WHEN ${metric} = 'Plan Results Non-Bounce %' THEN 17
           WHEN ${metric} = 'Wizard Sessions' THEN 18
           WHEN ${metric} = 'Wizard Conversions %' THEN 19
           WHEN ${metric} = 'Medigap Sessions' THEN 20
           WHEN ${metric} = 'Medigap Conversion %' THEN 21
           WHEN ${metric} = 'Online Enrollments' THEN 22
           WHEN ${metric} = 'Call Center Enrollments' THEN 23
           WHEN ${metric} = 'Total Enrollments' THEN 24
           WHEN ${metric} = 'Overall CSAT' THEN 25
           WHEN ${metric} = 'Goal Completion %' THEN 26
           WHEN ${metric} = 'Will Contact Call Center' THEN 27
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

  measure: total_metric {
    type: sum
    sql: cast(${TABLE}.values_2020 AS float)    ;;
  }


  set: detail {
    fields: [week_of_year, metric, weekly_totals]
  }
}
