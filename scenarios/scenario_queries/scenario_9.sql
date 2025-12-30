-- WITH newsletter_daily AS (
--   SELECT 
--     date,
--     SUM(visits) AS visits
--   FROM prod.newsletter
--   WHERE site_id = 4
--   GROUP BY date
-- ),
-- week_boundaries AS (
--   SELECT
--     CURDATE() AS today,
--     DAYOFWEEK(CURDATE()) AS day_of_week,
--     DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 DAY) AS current_monday,
--     DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 7 DAY) AS last_monday,
--     DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 14 DAY) AS second_last_monday,
--     DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 21 DAY) AS third_last_monday
-- ),
-- weekly_newsletter_visits AS (
--   SELECT 
--     'week_1' AS week_label,
--     SUM(v.visits) AS total_visits
--   FROM newsletter_daily v
--   JOIN week_boundaries wb ON 1=1
--   WHERE v.date >= wb.current_monday AND v.date <= wb.today

--   UNION ALL

--   SELECT 
--     'week_2',
--     SUM(v.visits)
--   FROM newsletter_daily v
--   JOIN week_boundaries wb ON 1=1
--   WHERE v.date >= wb.last_monday AND v.date < wb.current_monday

--   UNION ALL

--   SELECT 
--     'week_3',
--     SUM(v.visits)
--   FROM newsletter_daily v
--   JOIN week_boundaries wb ON 1=1
--   WHERE v.date >= wb.second_last_monday AND v.date < wb.last_monday
-- )
-- SELECT
--   MAX(CASE WHEN week_label = 'week_3' THEN total_visits END) AS week_3_visits,
--   MAX(CASE WHEN week_label = 'week_2' THEN total_visits END) AS week_2_visits,
--   MAX(CASE WHEN week_label = 'week_1' THEN total_visits END) AS week_1_visits,
--   CASE 
--     WHEN 
--       MAX(CASE WHEN week_label = 'week_3' THEN total_visits END) < 
--       MAX(CASE WHEN week_label = 'week_2' THEN total_visits END) AND
--       MAX(CASE WHEN week_label = 'week_2' THEN total_visits END) < 
--       MAX(CASE WHEN week_label = 'week_1' THEN total_visits END)
--     THEN 'True'
--     ELSE 'False'
--   END AS is_upward_trend
-- FROM weekly_newsletter_visits;


WITH newsletter_daily AS (
  SELECT 
    date,
    SUM(visits) AS visits
  FROM prod.newsletter
  WHERE site_id = %(siteid)s
  GROUP BY date
),
week_boundaries AS (
  SELECT
    CURDATE() AS today,
    DAYOFWEEK(CURDATE()) AS day_of_week,
    DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 DAY) AS current_monday,
    DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 7 DAY) AS last_monday,
    DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 14 DAY) AS second_last_monday,
    DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 21 DAY) AS third_last_monday
),
weekly_newsletter_visits AS (
  SELECT 
    'week_1' AS week_label,
    SUM(v.visits) AS total_visits
  FROM newsletter_daily v
  JOIN week_boundaries wb ON 1=1
  WHERE v.date >= wb.current_monday AND v.date <= wb.today

  UNION ALL

  SELECT 
    'week_2',
    SUM(v.visits)
  FROM newsletter_daily v
  JOIN week_boundaries wb ON 1=1
  WHERE v.date >= wb.last_monday AND v.date < wb.current_monday

  UNION ALL

  SELECT 
    'week_3',
    SUM(v.visits)
  FROM newsletter_daily v
  JOIN week_boundaries wb ON 1=1
  WHERE v.date >= wb.second_last_monday AND v.date < wb.last_monday
)
SELECT
  CASE 
    WHEN 
      MAX(CASE WHEN week_label = 'week_3' THEN total_visits END) < 
      MAX(CASE WHEN week_label = 'week_2' THEN total_visits END) AND
      MAX(CASE WHEN week_label = 'week_2' THEN total_visits END) < 
      MAX(CASE WHEN week_label = 'week_1' THEN total_visits END)
    THEN 'True'
    ELSE 'False'
  END AS status
FROM weekly_newsletter_visits;
