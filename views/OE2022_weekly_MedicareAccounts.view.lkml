view: oe2022_weekly_medicareaccounts {
derived_table: {
  sql: --Medicare accounts
WITH

sessions AS (SELECT EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', event_date)) AS Week
    ,event_date
    ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', event_date)) AS Year
    ,user_pseudo_id
    ,CASE WHEN ep.key = 'ga_session_id' THEN CONCAT(user_pseudo_id, cast(ep.value.int_value as string)) END AS sessionId
    ,COUNTIF(event_name='page_view' AND ep.key = 'page_location') AS pageviews
    FROM `steady-cat-772.analytics_266429760.events_*`
    ,UNNEST(event_params) AS ep
    WHERE (_TABLE_SUFFIX BETWEEN '20211201' AND '20211201' OR _TABLE_SUFFIX BETWEEN '20201201' AND '20201201')
      AND  (REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), '/mbp/') OR
            REGEXP_CONTAINS((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location'), '/account/'))
    GROUP BY Week, Year, user_pseudo_id, sessionId, event_date
)

, accounts AS (
    SELECT EXTRACT(WEEK FROM PARSE_DATE('%Y-%m-%d', date)) AS Week
    ,EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', date)) AS Year
    ,SUM(CAST(REGEXP_REPLACE(NewAccounts, ',', '') AS FLOAT64)) AS `New Accounts`
    ,SUM(CAST(REGEXP_REPLACE(SuccessfulLogins, ',', '') AS FLOAT64)) AS `Successful Logins`
    ,ROUND(SUM(CAST(REGEXP_REPLACE(SuccessfulLogins, ',', '') AS FLOAT64)) /
        (SUM(CAST(REGEXP_REPLACE(SuccessfulLogins, ',', '') AS FLOAT64)) + SUM(CAST(REGEXP_REPLACE(FailedLogins, ',', '') AS FLOAT64))) * 100) AS `% Login Success`
    FROM `steady-cat-772.CMSGoogleSheets.MedicareAccountsTable`
    WHERE (date BETWEEN '2021-12-01' AND '2021-12-01' OR date BETWEEN '2020-12-01' AND '2020-12-01')
    GROUP BY Week, Year
)

, temp_2021 AS (SELECT Week
    ,Year
    ,CONCAT(MIN(PARSE_DATE('%Y%m%d', event_date)), ' - ', MAX(PARSE_DATE('%Y%m%d', event_date))) AS date_range
    ,CAST(COUNT(DISTINCT user_pseudo_id) AS FLOAT64) AS `Users`
    ,CAST(COUNT(DISTINCT sessionId) AS FLOAT64) AS `Sessions`
    ,CAST(SUM(pageviews) AS FLOAT64) AS `Pageviews`
    FROM sessions
    WHERE Year = 2021
    GROUP BY Week, Year
)

, temp_2020 AS (SELECT Week
   ,Year
   ,CONCAT(MIN(PARSE_DATE('%Y%m%d', event_date)), ' - ', MAX(PARSE_DATE('%Y%m%d', event_date))) AS date_range
   ,CAST(COUNT(DISTINCT user_pseudo_id) AS FLOAT64) AS `Users`
   ,CAST(COUNT(DISTINCT sessionId) AS FLOAT64) AS `Sessions`
   ,CAST(SUM(pageviews) AS FLOAT64) AS `Pageviews`
   FROM sessions
   WHERE Year = 2020
   GROUP BY Week, Year
)

, agg_2021 AS (SELECT temp_2021.*, accounts.`New Accounts`, accounts.`Successful Logins`, accounts.`% Login Success`
    FROM temp_2021
    LEFT JOIN accounts ON accounts.Week = temp_2021.Week AND accounts.Year = temp_2021.Year
)

, agg_2020 AS (SELECT temp_2020.*, accounts.`New Accounts`, accounts.`Successful Logins`, accounts.`% Login Success`
   FROM temp_2020
   LEFT JOIN accounts ON accounts.Week = temp_2020.Week AND accounts.Year = temp_2020.Year
)

,t_2021 AS (SELECT *
    FROM agg_2021
    UNPIVOT(values_2021 FOR metric IN (`Users`, `Sessions`, `Pageviews`, `New Accounts`, `Successful Logins`, `% Login Success`))
)

,t_2020 AS (SELECT *
   FROM agg_2020
   UNPIVOT(values_2020 FOR metric IN (`Users`, `Sessions`, `Pageviews`, `New Accounts`, `Successful Logins`, `% Login Success`))
)

SELECT CONCAT('Week ', t_2021.Week - 40) AS Week, t_2021.date_range, t_2021.metric
,CASE WHEN t_2021.metric IN ('Users', 'Sessions', 'Pageviews', 'New Accounts', 'Successful Logins') THEN CONCAT(FORMAT("%'d", CAST(values_2021 AS int64)))
        WHEN t_2021.metric = 'Sessions per User' THEN CONCAT(FORMAT("%'d", CAST(values_2021 AS int64)), SUBSTR(FORMAT("%.2f", CAST(values_2021 AS float64)), -3))
        ELSE CONCAT(values_2021, '%') END as values_2021
,CONCAT(ROUND((values_2021 - LAG(values_2021, 1, NULL) OVER (PARTITION BY t_2021.metric ORDER BY t_2021.Week)) /
        LAG(values_2021, 1, NULL) OVER (PARTITION BY t_2021.metric ORDER BY t_2021.Week) * 100), '%') AS prev_week
,CASE WHEN t_2021.metric IN ('Users', 'Sessions', 'Pageviews', 'New Accounts', 'Successful Logins') THEN CONCAT(FORMAT("%'d", CAST(values_2020 AS int64)))
       WHEN t_2021.metric = 'Sessions per User' THEN CONCAT(FORMAT("%'d", CAST(values_2020 AS int64)), SUBSTR(FORMAT("%.2f", CAST(values_2020 AS float64)), -3))
       ELSE CONCAT(values_2020, '%') END as values_2020
   ,CONCAT(ROUND(SAFE_DIVIDE(values_2021 - values_2020, values_2020)*100), '%') AS Perc_Change_YoY
FROM t_2021
LEFT JOIN t_2020
ON t_2020.Week = t_2021.Week AND t_2020.metric = t_2021.metric

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
