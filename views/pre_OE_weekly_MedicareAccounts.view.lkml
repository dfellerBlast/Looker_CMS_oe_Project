view: pre_oe_weekly_medicareaccounts {
  derived_table: {
    sql: WITH sessions AS (SELECT EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', date)) AS Week
      ,date
      ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', date)) AS Year
      ,fullVisitorId
      ,concat(fullVisitorId, visitId, date) AS sessionId
      ,COUNTIF(hits.type = 'PAGE') AS pageviews
      FROM `steady-cat-772.157906096.ga_sessions_20*`
      ,UNNEST(hits) AS hits
      WHERE (_TABLE_SUFFIX BETWEEN '201015' AND '201207' OR _TABLE_SUFFIX BETWEEN '191015' AND '191207')
      -- WHERE (_TABLE_SUFFIX BETWEEN '201015' AND '201030' OR _TABLE_SUFFIX BETWEEN '191015' AND '191030')
      AND REGEXP_CONTAINS(hits.page.pagePath, '/account/')
      GROUP BY Week, Year, fullVisitorId, visitId, date
      )
      -- SELECT * FROM sessions
      , accounts AS (
          SELECT EXTRACT(WEEK FROM PARSE_DATE('%Y-%m-%d', date)) AS Week
          ,EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', date)) AS Year
          ,SUM(CAST(REGEXP_REPLACE(NewAccounts, ',', '') AS FLOAT64)) AS `New Accounts`
          ,SUM(CAST(REGEXP_REPLACE(SuccessfulLogins, ',', '') AS FLOAT64)) AS `Successful Logins`
          ,ROUND(SUM(CAST(REGEXP_REPLACE(SuccessfulLogins, ',', '') AS FLOAT64)) /
              (SUM(CAST(REGEXP_REPLACE(SuccessfulLogins, ',', '') AS FLOAT64)) + SUM(CAST(REGEXP_REPLACE(FailedLogins, ',', '') AS FLOAT64))) * 100) AS `% Login Success`
          FROM `steady-cat-772.CMSGoogleSheets.MedicareAccountsTable`
          WHERE date >= '2020-10-15'
          GROUP BY Week, Year
      )
      -- SELECT * FROM accounts
      , temp AS (SELECT Week
      ,Year
      ,CONCAT(MIN(PARSE_DATE('%Y%m%d', date)), ' - ', MAX(PARSE_DATE('%Y%m%d', date))) AS date_range
      ,CAST(COUNT(DISTINCT fullVisitorId) AS FLOAT64) AS `Users`
      ,CAST(COUNT(DISTINCT sessionId) AS FLOAT64) AS `Sessions`
      ,CAST(SUM(pageviews) AS FLOAT64) AS `Pageviews`
      FROM sessions
      GROUP BY Week, Year)
      -- SELECT * FROM temp
      , agg AS (SELECT temp.*, accounts.`New Accounts`, accounts.`Successful Logins`, accounts.`% Login Success`
          FROM temp
          LEFT JOIN accounts ON accounts.Week = temp.Week AND accounts.Year = temp.Year
      )
      -- SELECT * FROM agg
      ,t_2020 AS (SELECT *
      FROM agg
      UNPIVOT(values_2020 FOR metric IN (`Users`, `Sessions`, `Pageviews`, `New Accounts`, `Successful Logins`, `% Login Success`))
      WHERE year = 2020
      )
      ,t_2019 AS (SELECT *
      FROM agg
      UNPIVOT(values_2019 FOR metric IN (`Users`, `Sessions`, `Pageviews`, `New Accounts`, `Successful Logins`, `% Login Success`))
      WHERE year = 2019
      )
      SELECT CONCAT('Week ', t_2020.Week - 40) AS Week, t_2020.date_range, t_2020.metric
      ,CASE WHEN t_2020.metric IN ('Users', 'Sessions', 'Pageviews', 'New Accounts', 'Successful Logins') THEN CONCAT(FORMAT("%'d", CAST(values_2020 AS int64)))
              WHEN t_2020.metric = 'Sessions per User' THEN CONCAT(FORMAT("%'d", CAST(values_2020 AS int64)), SUBSTR(FORMAT("%.2f", CAST(values_2020 AS float64)), -3))
              ELSE CONCAT(values_2020, '%') END as values_2020
      ,CONCAT(ROUND((values_2020 - LAG(values_2020, 1, NULL) OVER (PARTITION BY t_2020.metric ORDER BY t_2020.Week)) /
              LAG(values_2020, 1, NULL) OVER (PARTITION BY t_2020.metric ORDER BY t_2020.Week) * 100), '%') AS prev_week
      ,CASE WHEN t_2020.metric IN ('Users', 'Sessions', 'Pageviews', 'New Accounts', 'Successful Logins') THEN CONCAT(FORMAT("%'d", CAST(values_2019 AS int64)))
              WHEN t_2020.metric = 'Sessions per User' THEN CONCAT(FORMAT("%'d", CAST(values_2019 AS int64)), SUBSTR(FORMAT("%.2f", CAST(values_2019 AS float64)), -3))
              ELSE CONCAT(values_2019, '%') END as values_2019
          ,CONCAT(ROUND(SAFE_DIVIDE(values_2020 - values_2019, values_2019)*100), '%') AS Perc_Change_YoY
      FROM t_2020 LEFT JOIN t_2019 ON t_2019.Week = t_2020.Week AND t_2019.metric = t_2020.metric
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
    sql: ${TABLE}.values_2020 ;;
  }

  dimension: previous_week {
    type: string
    sql: ${TABLE}.prev_week ;;
  }

  dimension: 2019_Weekly_Totals {
    type: string
    sql: ${TABLE}.values_2019 ;;
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
