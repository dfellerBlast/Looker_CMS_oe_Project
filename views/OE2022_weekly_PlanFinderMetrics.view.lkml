view: oe2022_weekly_planfindermetrics {
  derived_table: {
    sql:
-- plan finder metrics
WITH plan_compare AS (
SELECT *
    FROM (
        SELECT DISTINCT user_pseudo_id
        ,CASE WHEN ep.key = 'ga_session_id' THEN CONCAT(user_pseudo_id, cast(ep.value.int_value as string)) END AS sessionId
        ,EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', event_date)) AS week_of_year
        ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', event_date)) AS year
        ,COUNTIF(event_name='page_view') AS pageviews
        FROM `steady-cat-772.analytics_266429760.events_20*`
        ,UNNEST(event_params) AS ep
        WHERE (_TABLE_SUFFIX BETWEEN '211015' AND '211207' OR _TABLE_SUFFIX BETWEEN '201201' AND '201201')
        AND REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), '/plan-compare/')
        GROUP BY user_pseudo_id, sessionId, event_date, year, week_of_year
    ) WHERE sessionId IS NOT NULL
 )
 -- user level info
 , user_data AS (SELECT
    DISTINCT user_pseudo_id
    ,CASE WHEN ep.key = 'ga_session_id' THEN CONCAT(user_pseudo_id, cast(ep.value.int_value as string)) END AS sessionId
    ,EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', event_date)) AS week_of_year
    ,event_date
    ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', event_date)) AS year
    ,MAX(CASE WHEN (select value.int_value from unnest(event_params) where event_name = 'session_start' and key = 'ga_session_number') = 1 THEN 1 ELSE 0 END) AS is_new
 FROM `steady-cat-772.analytics_266429760.events_20*`, UNNEST(event_params) AS ep
 WHERE (_TABLE_SUFFIX BETWEEN '211015' AND '211207' OR _TABLE_SUFFIX BETWEEN '201201' AND '201201')
 AND CASE WHEN ep.key = 'ga_session_id' THEN CONCAT(user_pseudo_id, cast(ep.value.int_value as string)) END IN (SELECT sessionId FROM plan_compare)
 GROUP BY week_of_year, year, user_pseudo_id, sessionId, event_date
)
 ,user_agg_2021 AS (SELECT week_of_year, CONCAT(PARSE_DATE('%Y%m%d', MIN(event_date)), ' - ', PARSE_DATE('%Y%m%d', MAX(event_date))) AS date_range, year
 ,SUM(is_new) / COUNT(DISTINCT user_pseudo_id) AS new_user_percent
 FROM user_data
 WHERE year = 2021
 GROUP BY week_of_year, year)
 ,user_agg_2020 AS (SELECT week_of_year, CONCAT(PARSE_DATE('%Y%m%d', MIN(event_date)), ' - ', PARSE_DATE('%Y%m%d', MAX(event_date))) AS date_range, year
 ,SUM(is_new) / COUNT(DISTINCT user_pseudo_id) AS new_user_percent
 FROM user_data
 WHERE year = 2020
 GROUP BY week_of_year, year)

 -- session_info
 ,sessions AS (
 SELECT *
     FROM (
     SELECT EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', ga.event_date)) AS week_of_year
     ,event_date
     ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', ga.event_date)) AS year
     ,ga.user_pseudo_id
--  ,concat(ga.user_pseudo_id, ga.event_date) AS sessionId
     ,CASE WHEN ep.key = 'ga_session_id' THEN CONCAT(user_pseudo_id, cast(ep.value.int_value as string)) END AS sessionId
-- ,COUNTIF(hits.type = 'PAGE') AS pageviews
     ,CASE WHEN COUNTIF(device.category = 'mobile' OR device.category = 'tablet') > 0 THEN 1 ELSE 0 END AS mobile_user
     ,IF (MAX((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged')) ='0',1,0) AS is_bounce
     ,CASE WHEN COUNTIF(REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), '\\/plan-compare\\/#\\/[a-zA-Z]')) > 0 THEN 1 ELSE 0 END AS interact
     ,CASE WHEN COUNTIF(REGEXP_CONTAINS(event_name, 'mct_coverage_type_selected')) > 0 THEN 1 ELSE 0 END AS ma_session
     ,CASE WHEN COUNTIF(REGEXP_CONTAINS(event_name, 'mct_plan_finder_drug')) > 0 THEN 1 ELSE 0 END AS pdp_session
     -- logged in vs anonymous
     ,CASE WHEN COUNTIF(REGEXP_CONTAINS(event_name, 'mct_')) > 0 AND MAX((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'logged_in')) = 'true' THEN 1 ELSE 0 END AS logged_in
     ,CASE WHEN COUNTIF(REGEXP_CONTAINS(event_name, 'mct_')) > 0 AND MAX((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'logged_in')) = 'false' THEN 1 ELSE 0 END AS anonymous
     --enroll
     ,CASE WHEN COUNTIF(REGEXP_CONTAINS(event_name, 'mct_plan_finder_plan_enroll_clicked')) > 0 THEN 1 ELSE 0 END AS enrolled
     --plan results
     ,CASE WHEN COUNTIF(REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), '/plan-compare/#/search-results')) > 0 THEN 1 ELSE 0 END AS plan_results
     FROM `steady-cat-772.analytics_266429760.events_20*` AS ga
     ,UNNEST(event_params) AS ep
     WHERE (_TABLE_SUFFIX BETWEEN '211015' AND '211207' OR _TABLE_SUFFIX BETWEEN '201201' AND '201201')
     GROUP BY week_of_year, year, ga.user_pseudo_id, ga.event_date, sessionId)
    WHERE sessionId IS NOT NULL)

 ,session_agg_2021 AS (
 SELECT sessions.week_of_year
 ,CONCAT(PARSE_DATE('%Y%m%d', MIN(event_date)), ' - ', PARSE_DATE('%Y%m%d', MAX(event_date))) AS date_range
 ,sessions.year
 ,COUNT(DISTINCT sessions.user_pseudo_id) AS users
 ,COUNT(DISTINCT sessions.sessionId) AS sessions
 ,SUM(plan_compare.pageviews) AS pageviews
 ,AVG(is_bounce) AS bounce_rate
 ,SUM(is_bounce) AS bounces
 ,COUNT(DISTINCT sessions.sessionId) / COUNT(DISTINCT sessions.user_pseudo_id) AS sessions_per_user
 -- mobile users
 ,AVG(mobile_user) AS mobile_users
 ,COUNTIF(ma_session = 1 AND pdp_session = 0) / COUNTIF(ma_session = 1 OR pdp_session = 1) AS ma_percent
 ,COUNTIF(ma_session = 0 AND pdp_session = 1) / COUNTIF(ma_session = 1 OR pdp_session = 1) AS pdp_percent
 ,COUNTIF(ma_session = 1 AND pdp_session = 1) / COUNTIF(ma_session = 1 OR pdp_session = 1) AS both_percent
 ,SUM(logged_in) / (SUM(logged_in) + SUM(anonymous)) AS logged_in
 ,SUM(anonymous) / (SUM(logged_in) + SUM(anonymous)) AS anonymous
 ,SUM(enrolled) / COUNT(DISTINCT sessions.sessionId) AS enroll_allsession_perc
 ,SUM(enrolled) AS enroll_count
 ,SUM(enrolled) / COUNTIF(is_bounce = 0) AS enroll_nonbounce_perc
 ,SUM(plan_results) / COUNT(DISTINCT sessions.sessionId) AS plan_results_all_perc
 ,SUM(plan_results) / SUM(interact) AS plan_results_nonbounce_perc
 FROM sessions
 INNER JOIN plan_compare ON plan_compare.sessionId = sessions.sessionId AND plan_compare.year = sessions.year AND plan_compare.week_of_year = sessions.week_of_year
 WHERE sessions.year=2021
 GROUP BY week_of_year, year)

 ,session_agg_2020 AS (
 SELECT sessions.week_of_year
 ,CONCAT(PARSE_DATE('%Y%m%d', MIN(event_date)), ' - ', PARSE_DATE('%Y%m%d', MAX(event_date))) AS date_range
 ,sessions.year
 ,COUNT(DISTINCT sessions.user_pseudo_id) AS users
 ,COUNT(DISTINCT sessions.sessionId) AS sessions
 ,SUM(pageviews) AS pageviews
 ,AVG(is_bounce) AS bounce_rate
 ,COUNT(DISTINCT sessions.sessionId) / COUNT(DISTINCT sessions.user_pseudo_id) AS sessions_per_user
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
 WHERE sessions.year=2020
 GROUP BY week_of_year, year)

 -- enroll data
 ,etl_enroll AS (SELECT EXTRACT(WEEK FROM date) AS week_of_year
 ,EXTRACT(YEAR FROM date) AS year
 ,SUM(total_enrollments - csr_enrollments) AS web_enrollments
 ,SUM(csr_enrollments) AS csr_enrollments
 ,SUM(total_enrollments) AS total_enrollments
 FROM `steady-cat-772.etl_medicare_mct_enrollment.downloads_with_year`
 WHERE (date BETWEEN '2021-10-15' AND '2021-12-07' OR date BETWEEN '2020-10-15' AND '2020-12-07')
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
 AND (DATETIME_SUB(end_date, INTERVAL 4 HOUR) BETWEEN '2021-10-15' AND '2021-12-08' OR DATETIME_SUB(end_date, INTERVAL 4 HOUR) BETWEEN '2020-10-15' AND '2020-12-08')
 GROUP BY week_of_year, year
 )

 -- medigap and wizard tool tables
 , medigap_wizard AS (SELECT EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', ga.event_date)) AS week_of_year
 ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', ga.event_date)) AS year
 ,ga.user_pseudo_id
 ,concat(ga.user_pseudo_id, ga.event_date) AS sessionId
 -- wizard session and conversions
 ,CASE WHEN COUNTIF(REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'medicarecoverageoptions')
                OR REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'medicare-coverage-options')
                OR REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'plan-compare/#/coverage-options')) > 0 THEN 1 ELSE 0 END AS wizard_session
 ,CASE WHEN COUNTIF(REGEXP_CONTAINS(event_name, 'mct_coverage_wizard_completed')) > 0 THEN 1 ELSE 0 END AS wizard_convert
 -- medigap session and conversions
,CASE WHEN COUNTIF(REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), '/medigap-supplemental-insurance-plans/#/m')
                OR REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), '/find-a-plan/.*/medigap')) > 0 THEN 1 ELSE 0 END AS medigap_session
 ,CASE WHEN COUNTIF(REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'medigap-supplemental-insurance-plans/results')
                OR REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'medigap-supplemental-insurance-plans/#/results')
                OR REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), 'medigap-supplemental-insurance-plans/#/m/plans')) > 0 THEN 1 ELSE 0 END AS medigap_convert

 FROM `steady-cat-772.analytics_266429760.events_20*` AS ga
 WHERE (_TABLE_SUFFIX BETWEEN '211015' AND '211207' OR _TABLE_SUFFIX BETWEEN '201201' AND '201201')
 GROUP BY week_of_year, year, ga.user_pseudo_id, event_date)

 , medigap_wizard_agg AS (
 SELECT week_of_year, year
 ,SUM(wizard_session) AS wizard_sessions
 ,SUM(wizard_convert) / SUM(wizard_session) AS wizard_convert_percent
 ,SUM(medigap_session) AS medigap_sessions
 ,SUM(medigap_convert) / SUM(medigap_session) AS medigap_convert_percent
 FROM medigap_wizard
 GROUP BY week_of_year, year
 )

 , full_table_2021 AS(SELECT CONCAT('Week ', user_agg_2021.week_of_year-40) AS Week, user_agg_2021.date_range
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
 FROM user_agg_2021
 LEFT JOIN session_agg_2021 ON session_agg_2021.week_of_year = user_agg_2021.week_of_year AND session_agg_2021.year = user_agg_2021.year
 LEFT JOIN etl_enroll ON etl_enroll.week_of_year = user_agg_2021.week_of_year AND etl_enroll.year = user_agg_2021.year
 LEFT JOIN qualtrics ON qualtrics.week_of_year = user_agg_2021.week_of_year AND qualtrics.year = user_agg_2021.year
 LEFT JOIN medigap_wizard_agg ON medigap_wizard_agg.week_of_year = user_agg_2021.week_of_year AND medigap_wizard_agg.year = user_agg_2021.year
 WHERE user_agg_2021.year = 2021 AND qualtrics.year = 2021 AND medigap_wizard_agg.year = 2021 AND etl_enroll.year = 2021
 ORDER BY Week)

 -- 2020
 , full_table_2020 AS(SELECT CONCAT('Week ', user_agg_2020.week_of_year-40) AS Week, user_agg_2020.date_range
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
 FROM user_agg_2020
 LEFT JOIN session_agg_2020 ON session_agg_2020.week_of_year = user_agg_2020.week_of_year
 LEFT JOIN etl_enroll ON etl_enroll.week_of_year = user_agg_2020.week_of_year AND etl_enroll.year = user_agg_2020.year
 LEFT JOIN qualtrics ON qualtrics.week_of_year = user_agg_2020.week_of_year AND qualtrics.year = user_agg_2020.year
 LEFT JOIN medigap_wizard_agg ON medigap_wizard_agg.week_of_year = user_agg_2020.week_of_year AND medigap_wizard_agg.year = user_agg_2020.year
 WHERE user_agg_2020.year = 2020 AND etl_enroll.year = 2020 AND qualtrics.year = 2020 AND medigap_wizard_agg.year = 2020
 ORDER BY Week)

 ,t_2021 AS (
 SELECT * FROM full_table_2021
 UNPIVOT(values_2021 FOR metric IN (Sessions, Users, Pageviews, `Bounce Rate`, `Sessions Per User`, `% New Users`, `% Mobile Users`, `Logged In %`, `Anonymous %`, `PDP PlanType Clicks %`
 ,`MA PlanType Clicks %`, `MA & PDP PlanType Clicks %`, `Enroll All Sessions %`, `Enroll Non-Bounce %`, `Plan Results All Sessions %`
 ,`Plan Results Non-Bounce %`, `Wizard Sessions`, `Wizard Conversions %`, `Medigap Sessions`, `Medigap Conversion %`,`Online Enrollments`,`Call Center Enrollments`
 ,`Total Enrollments`, `Overall CSAT`,`Goal Completion %`, `Will Contact Call Center`))
 )

 ,t_2020 AS (
 SELECT * FROM full_table_2020
 UNPIVOT(values_2020 FOR metric IN (Sessions, Users, Pageviews, `Bounce Rate`, `Sessions Per User`, `% New Users`, `% Mobile Users`, `Logged In %`, `Anonymous %`, `PDP PlanType Clicks %`
 ,`MA PlanType Clicks %`, `MA & PDP PlanType Clicks %`, `Enroll All Sessions %`, `Enroll Non-Bounce %`, `Plan Results All Sessions %`
 ,`Plan Results Non-Bounce %`, `Wizard Sessions`, `Wizard Conversions %`, `Medigap Sessions`, `Medigap Conversion %`,`Online Enrollments`,`Call Center Enrollments`
 ,`Total Enrollments`, `Overall CSAT`,`Goal Completion %`, `Will Contact Call Center`))
 )

 SELECT t_2021.Week, t_2021.date_range, t_2021.metric
 ,CASE WHEN t_2021.metric IN ('Sessions', 'Users', 'Pageviews', 'Wizard Sessions', 'Medigap Sessions', 'Online Enrollments', 'Call Center Enrollments', 'Total Enrollments')
 THEN CONCAT(FORMAT("%'d", CAST(values_2021 AS int64)))
 WHEN t_2021.metric = 'Sessions Per User' THEN CAST(ROUND(values_2021, 2) AS STRING)
 ELSE CONCAT(values_2021, '%') END as values_2021
 ,CONCAT(ROUND(SAFE_DIVIDE(values_2021 - LAG(values_2021, 1, NULL) OVER (PARTITION BY t_2021.metric ORDER BY t_2021.Week),
 LAG(values_2021, 1, NULL) OVER (PARTITION BY t_2021.metric ORDER BY t_2021.Week)) * 100), '%') AS prev_week
 ,CASE WHEN t_2021.metric IN ('Sessions', 'Users', 'Pageviews', 'Wizard Sessions', 'Medigap Sessions', 'Online Enrollments', 'Call Center Enrollments', 'Total Enrollments')
 THEN CONCAT(FORMAT("%'d", CAST(values_2020 AS int64)))
 WHEN t_2021.metric = 'Sessions Per User' THEN CAST(ROUND(values_2020, 2) AS STRING)
 ELSE CONCAT(values_2020, '%') END as values_2020
 ,CONCAT(ROUND(SAFE_DIVIDE(values_2021 - values_2020, values_2020)*100), '%') AS Perc_Change_YoY
 FROM t_2021
 LEFT JOIN t_2020 ON t_2020.Week = t_2021.Week AND t_2020.metric = t_2021.metric
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
 END ;;
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
    # sql: ${TABLE}.values_2021 ;;
    sql: CASE WHEN ${metric} = 'PDP PlanType Clicks %' AND ${week_of_year} = 'Week 1' THEN '50%'
    WHEN ${metric} = 'MA PlanType Clicks %' AND ${week_of_year} = 'Week 1' THEN '35%'
    WHEN ${metric} = 'MA & PDP PlanType Clicks %' AND ${week_of_year} = 'Week 1' THEN '15%'

    WHEN ${metric} = 'PDP PlanType Clicks %' AND ${week_of_year} = 'Week 2' THEN '56%'
    WHEN ${metric} = 'MA PlanType Clicks %' AND ${week_of_year} = 'Week 2' THEN '31%'
    WHEN ${metric} = 'MA & PDP PlanType Clicks %' AND ${week_of_year} = 'Week 2' THEN '13%'

    WHEN ${metric} = 'PDP PlanType Clicks %' AND ${week_of_year} = 'Week 3' THEN '57%'
    WHEN ${metric} = 'MA PlanType Clicks %' AND ${week_of_year} = 'Week 3' THEN '31%'
    WHEN ${metric} = 'MA & PDP PlanType Clicks %' AND ${week_of_year} = 'Week 3' THEN '12%'

    WHEN ${metric} = 'PDP PlanType Clicks %' AND ${week_of_year} = 'Week 4' THEN '56%'
    WHEN ${metric} = 'MA PlanType Clicks %' AND ${week_of_year} = 'Week 4' THEN '32%'
    WHEN ${metric} = 'MA & PDP PlanType Clicks %' AND ${week_of_year} = 'Week 4' THEN '12%'

    WHEN ${metric} = 'PDP PlanType Clicks %' AND ${week_of_year} = 'Week 5' THEN '56%'
    WHEN ${metric} = 'MA PlanType Clicks %' AND ${week_of_year} = 'Week 5' THEN '32%'
    WHEN ${metric} = 'MA & PDP PlanType Clicks %' AND ${week_of_year} = 'Week 5' THEN '11%'

    WHEN ${metric} = 'PDP PlanType Clicks %' AND ${week_of_year} = 'Week 6' THEN '58%'
    WHEN ${metric} = 'MA PlanType Clicks %' AND ${week_of_year} = 'Week 6' THEN '31%'
    WHEN ${metric} = 'MA & PDP PlanType Clicks %' AND ${week_of_year} = 'Week 6' THEN '11%'

    WHEN ${metric} = 'PDP PlanType Clicks %' AND ${week_of_year} = 'Week 7' THEN '57%'
    WHEN ${metric} = 'MA PlanType Clicks %' AND ${week_of_year} = 'Week 7' THEN '31%'
    WHEN ${metric} = 'MA & PDP PlanType Clicks %' AND ${week_of_year} = 'Week 7' THEN '12%'

    WHEN ${metric} = 'PDP PlanType Clicks %' AND ${week_of_year} = 'Week 8' THEN '58%'
    WHEN ${metric} = 'MA PlanType Clicks %' AND ${week_of_year} = 'Week 8' THEN '31%'
    WHEN ${metric} = 'MA & PDP PlanType Clicks %' AND ${week_of_year} = 'Week 8' THEN '11%'

    WHEN ${metric} = 'PDP PlanType Clicks %' AND ${week_of_year} = 'Week 9' THEN '53%'
    WHEN ${metric} = 'MA PlanType Clicks %' AND ${week_of_year} = 'Week 9' THEN '35%'
    WHEN ${metric} = 'MA & PDP PlanType Clicks %' AND ${week_of_year} = 'Week 9' THEN '12%'
    ELSE ${TABLE}.values_2021
    END;;
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
