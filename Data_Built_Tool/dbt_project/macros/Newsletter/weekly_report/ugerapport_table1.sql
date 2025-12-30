{% macro ugerapport_table1_dbt_newsletter() %}

{% set site = var('site') %}
{% set event_name = var('event_action')%}

CREATE PROCEDURE `ugerapport_table1_dbt_{{site}}`()
BEGIN

WITH

-- Date logic for full weeks
date_bounds AS (
  SELECT
    -- Last week's Monday and Sunday
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 7) DAY) AS last_monday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 1) DAY) AS last_sunday,
    -- Previous week's Monday and Sunday
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 14) DAY) AS prev_monday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 8) DAY) AS prev_sunday
),

-- CTA CTEs
last_week_events AS (
  SELECT date, SUM(hits) AS hits
  FROM prod.events, date_bounds
  WHERE siteid = {{site}} AND event_action = '{{event_name}}'
    AND date BETWEEN last_monday AND last_sunday
  GROUP BY date
),
last_week_pages AS (
  SELECT date, SUM(unique_pageviews) AS unique_pageviews
  FROM prod.pages, date_bounds
  WHERE siteid = {{site}}
    AND date BETWEEN last_monday AND last_sunday
  GROUP BY date
),
last_week_ratios AS (
  SELECT
    COALESCE(SUM(e.hits), 0) AS daily_ratio
  FROM last_week_events e
  JOIN last_week_pages p ON e.date = p.date
),
previous_week_events AS (
  SELECT date, SUM(hits) AS hits
  FROM prod.events, date_bounds
  WHERE siteid = {{site}} AND event_action = '{{event_name}}'
    AND date BETWEEN prev_monday AND prev_sunday
  GROUP BY date
),
previous_week_pages AS (
  SELECT date, SUM(unique_pageviews) AS unique_pageviews
  FROM prod.pages, date_bounds
  WHERE siteid = {{site}}
    AND date BETWEEN prev_monday AND prev_sunday
  GROUP BY date
),
previous_week_ratios AS (
  SELECT
    COALESCE(SUM(e.hits), 0) AS daily_ratio
  FROM previous_week_events e
  JOIN previous_week_pages p ON e.date = p.date
),
last_week_total AS (
  SELECT SUM(daily_ratio) AS total_ratio FROM last_week_ratios
),
previous_week_total AS (
  SELECT SUM(daily_ratio) AS total_ratio FROM previous_week_ratios
),
cta_goal_total AS (
  SELECT SUM(cta_per_day) AS cta_total
  FROM prod.goals, date_bounds
  WHERE site_id = {{site}} AND date BETWEEN last_monday AND last_sunday
),

-- BESOG CTE
besog_data AS (
  SELECT
    SUM(visits) AS total_visits,
    (SELECT SUM(visits_per_day)
     FROM prod.goals, date_bounds
     WHERE site_id = {{site}}
       AND date BETWEEN last_monday AND last_sunday
    ) AS goal_visits,
    (SELECT SUM(visits)
     FROM prod.daily_totals, date_bounds
     WHERE siteid = {{site}}
       AND date BETWEEN prev_monday AND prev_sunday
    ) AS previous_visits
  FROM prod.daily_totals, date_bounds
  WHERE siteid = {{site}}
    AND date BETWEEN last_monday AND last_sunday
),

-- SIDEVISINGER CTE
sidevisninger_data AS (
  SELECT
    SUM(unique_pageviews) AS total_pageviews,
    (SELECT SUM(pageviews_per_day)
     FROM prod.goals, date_bounds
     WHERE site_id = {{site}}
       AND date BETWEEN last_monday AND last_sunday
    ) AS goal_pageviews,
    (SELECT SUM(unique_pageviews)
     FROM prod.daily_totals, date_bounds
     WHERE siteid = {{site}}
       AND date BETWEEN prev_monday AND prev_sunday
    ) AS previous_pageviews
  FROM prod.daily_totals, date_bounds
  WHERE siteid = {{site}}
    AND date BETWEEN last_monday AND last_sunday
)

-- Final JSON Output
SELECT JSON_OBJECT(
  'site', {{site}},
  'data', JSON_OBJECT(
    'cta', JSON_OBJECT(
      'total', ROUND(l.total_ratio, 0),
      'afvigelse_ift_maal',
        CONCAT(
          CASE
            WHEN g.cta_total = 0 THEN '0.0'
            WHEN l.total_ratio / g.cta_total - 1 >= 0 THEN '+'
            ELSE '-'
          END,
          ROUND(ABS(l.total_ratio / g.cta_total - 1) * 100, 1), '%'
        ),
      'udvikling_ift_forrige_uge',
        CONCAT(
          CASE
            WHEN p.total_ratio = 0 THEN '0.0'
            WHEN l.total_ratio / p.total_ratio - 1 >= 0 THEN '+'
            ELSE '-'
          END,
          ROUND(ABS(l.total_ratio / p.total_ratio - 1) * 100, 1), '%'
        )
    ),
    'besog', JSON_OBJECT(
      'total', b.total_visits,
      'afvigelse_ift_maal', CONCAT(
        CASE WHEN b.goal_visits = 0 THEN ''
             WHEN ((b.total_visits - b.goal_visits) / b.goal_visits) >= 0 THEN '+'
             ELSE ''
        END,
        ROUND(
          CASE
            WHEN b.goal_visits = 0 THEN 0
            ELSE ((b.total_visits - b.goal_visits) / b.goal_visits) * 100
          END,
          1
        ),
        '%'
      ),
      'udvikling_ift_forrige_uge', CONCAT(
        CASE WHEN b.previous_visits = 0 THEN ''
             WHEN ((b.total_visits - b.previous_visits) / b.previous_visits) >= 0 THEN '+'
             ELSE ''
        END,
        ROUND(
          CASE
            WHEN b.previous_visits = 0 THEN 0
            ELSE ((b.total_visits - b.previous_visits) / b.previous_visits) * 100
          END,
          1
        ),
        '%'
      )
    ),
    'sidevisninger', JSON_OBJECT(
      'total', s.total_pageviews,
      'afvigelse_ift_maal', CONCAT(
        CASE WHEN s.goal_pageviews = 0 THEN ''
             WHEN ((s.total_pageviews - s.goal_pageviews) / s.goal_pageviews) >= 0 THEN '+'
             ELSE ''
        END,
        ROUND(
          CASE
            WHEN s.goal_pageviews = 0 THEN 0
            ELSE ((s.total_pageviews - s.goal_pageviews) / s.goal_pageviews) * 100
          END,
          1
        ),
        '%'
      ),
      'udvikling_ift_forrige_uge', CONCAT(
        CASE WHEN s.previous_pageviews = 0 THEN ''
             WHEN ((s.total_pageviews - s.previous_pageviews) / s.previous_pageviews) >= 0 THEN '+'
             ELSE ''
        END,
        ROUND(
          CASE
            WHEN s.previous_pageviews = 0 THEN 0
            ELSE ((s.total_pageviews - s.previous_pageviews) / s.previous_pageviews) * 100
          END,
          1
        ),
        '%'
      )
    )
  )
) AS json_output
FROM last_week_total l, previous_week_total p, cta_goal_total g, besog_data b, sidevisninger_data s;

END
{% endmacro %}