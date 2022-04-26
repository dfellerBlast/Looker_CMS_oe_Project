view: oe2022_weekly_homepage {
  derived_table: {
    sql: WITH sessions AS (SELECT EXTRACT(WEEK FROM PARSE_DATE('%Y%m%d', event_date)) AS Week
    ,event_date
    ,EXTRACT(YEAR FROM PARSE_DATE('%Y%m%d', event_date)) AS Year
    ,user_pseudo_id
    ,CASE WHEN ep.key = 'ga_session_id' THEN CONCAT(user_pseudo_id, cast(ep.value.int_value as string)) END AS sessionId
    ,COUNTIF(event_name='page_view' AND ep.key = 'page_location' AND ep.value.string_value IN ('https://www.medicare.gov/', 'https://www.medicare.gov')) AS pageviews
    ,CASE WHEN COUNTIF(device.category = 'mobile' OR device.category = 'tablet') > 0 THEN 1 ELSE 0 END AS mobile_user
    ,IF (MAX((SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'session_engaged')) ='0',1,0) AS is_bounce
    ,COUNTIF(event_name='session_start') AS is_entrance
    FROM `steady-cat-772.analytics_266429760.events_*`
    ,UNNEST(event_params) AS ep
    WHERE (_TABLE_SUFFIX BETWEEN '20211201' AND '20211201' OR _TABLE_SUFFIX BETWEEN '20201201' AND '20201201')
    AND REGEXP_CONTAINS((SELECT REGEXP_REPLACE(value.string_value, 'https://www.medicare.gov', '') FROM UNNEST(event_params) WHERE key = 'page_location'), '^/(\\?|$)')
    GROUP BY Week, Year, user_pseudo_id, sessionId, event_date
    )

    , agg AS (SELECT Week
    ,Year
    ,CONCAT(MIN(PARSE_DATE('%Y%m%d', event_date)), ' - ', MAX(PARSE_DATE('%Y%m%d', event_date))) AS date_range
    ,CAST(COUNT(DISTINCT user_pseudo_id) AS FLOAT64) AS `Users`
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
        END ;;
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
