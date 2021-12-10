view: oe_weekly_oe_to_date {derived_table: {
    sql:--OE to date
      WITH plan_compare AS (
                SELECT fullVisitorId, visitId, CONCAT(fullVisitorId, visitId, date) AS sessionId, CAST(hits.type = 'PAGE' AS INT64) AS pageview
                ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', date)) AS year
                ,EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', date)) AS Week
                FROM `steady-cat-772.30876903.ga_sessions_20*`
                ,UNNEST(hits) AS hits
                WHERE (_TABLE_SUFFIX BETWEEN '211015' AND '211207' OR _TABLE_SUFFIX BETWEEN '201015' AND '201207')
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
                WHERE (date BETWEEN '2021-10-15' AND '2021-12-07' OR date BETWEEN '2020-10-15' AND '2020-12-07')
                GROUP BY year
            )
            , accounts AS (
                SELECT EXTRACT(YEAR FROM PARSE_DATE('%Y-%m-%d', date)) AS Year
                ,SUM(CAST(REGEXP_REPLACE(NewAccounts, ',', '') AS FLOAT64)) AS NewAccounts
                ,SUM(CAST(REGEXP_REPLACE(SuccessfulLogins, ',', '') AS FLOAT64)) AS SuccessfulLogins
                FROM `steady-cat-772.CMSGoogleSheets.MedicareAccountsTable`
                WHERE (date BETWEEN '2021-10-15' AND '2021-12-07' OR date BETWEEN '2020-10-15' AND '2020-12-07')
                GROUP BY Year
            )
            , temp AS (SELECT pc.year AS Year
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
            ,t_2021 AS (SELECT *
            FROM temp
            UNPIVOT(values_2021 FOR metric IN (`PlanFinder Users`, `PlanFinder Sessions`, `PlanFinder Pageviews`, `Online Enrollments`, `Call Center Enrollments`, `Total Enrollments`, `New Accounts`, `Successful Logins`))
            WHERE year = 2021
            )
            ,t_2020 AS (SELECT *
            FROM temp
            UNPIVOT(values_2020 FOR metric IN (`PlanFinder Users`, `PlanFinder Sessions`, `PlanFinder Pageviews`, `Online Enrollments`, `Call Center Enrollments`, `Total Enrollments`, `New Accounts`, `Successful Logins`))
            WHERE year = 2020
            )
            SELECT t_2021.metric, FORMAT("%'d", SUM(values_2021)) AS values_2021, FORMAT("%'d", SUM(values_2020)) AS values_2020,
            CONCAT(ROUND((SUM(values_2021) - SUM(values_2020)) / SUM(values_2020) * 100), '%') AS YoY_Change
            FROM t_2021
            LEFT JOIN t_2020 ON t_2020.metric = t_2021.metric
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

  dimension: OE_to_Date_Total{
    type: string
    sql: ${TABLE}.values_2021 ;;
  }


  dimension: 2020_OE_to_Date_Total {
    type: string
    sql: ${TABLE}.values_2020 ;;
  }

  dimension: perc_change_yoy {
    type: string
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
