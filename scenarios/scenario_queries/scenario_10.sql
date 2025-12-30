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
--     DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 DAY) AS this_monday, -- current week start
--     DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 7 DAY) AS last_monday,
--     DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 14 DAY) AS wk_2_monday,
--     DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 21 DAY) AS wk_3_monday,
--     DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 28 DAY) AS wk_4_monday,
--     DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 35 DAY) AS wk_5_monday,
--     DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 42 DAY) AS wk_6_monday
-- ),
-- weekly_visits AS (
--   SELECT
--     CASE 
--       WHEN date >= wb.last_monday AND date < wb.this_monday THEN 'last_week'
--       WHEN date >= wb.wk_2_monday AND date < wb.last_monday THEN 'wk_2'
--       WHEN date >= wb.wk_3_monday AND date < wb.wk_2_monday THEN 'wk_3'
--       WHEN date >= wb.wk_4_monday AND date < wb.wk_3_monday THEN 'wk_4'
--       WHEN date >= wb.wk_5_monday AND date < wb.wk_4_monday THEN 'wk_5'
--       WHEN date >= wb.wk_6_monday AND date < wb.wk_5_monday THEN 'wk_6'
--     END AS week_label,
--     SUM(visits) AS total_visits
--   FROM newsletter_daily v
--   JOIN week_boundaries wb ON 1=1
--   WHERE date >= wb.wk_6_monday AND date < wb.this_monday
--   GROUP BY week_label
-- ),
-- peak_check AS (
--   SELECT
--     MAX(CASE WHEN week_label = 'last_week' THEN total_visits END) AS last_week_total,
--     MAX(CASE WHEN week_label != 'last_week' THEN total_visits END) AS prev_max
--   FROM weekly_visits
-- )
-- SELECT
--   last_week_total,
--   prev_max,
--   CASE 
--     WHEN last_week_total > prev_max * 1.10 THEN 'True'
--     ELSE 'False'
--   END AS peaked_last_week
-- FROM peak_check;

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
    DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 DAY) AS this_monday, -- current week start
    DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 7 DAY) AS last_monday,
    DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 14 DAY) AS wk_2_monday,
    DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 21 DAY) AS wk_3_monday,
    DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 28 DAY) AS wk_4_monday,
    DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 35 DAY) AS wk_5_monday,
    DATE_SUB(CURDATE(), INTERVAL DAYOFWEEK(CURDATE()) - 2 + 42 DAY) AS wk_6_monday
),
weekly_visits AS (
  SELECT
    CASE 
      WHEN date >= wb.last_monday AND date < wb.this_monday THEN 'last_week'
      WHEN date >= wb.wk_2_monday AND date < wb.last_monday THEN 'wk_2'
      WHEN date >= wb.wk_3_monday AND date < wb.wk_2_monday THEN 'wk_3'
      WHEN date >= wb.wk_4_monday AND date < wb.wk_3_monday THEN 'wk_4'
      WHEN date >= wb.wk_5_monday AND date < wb.wk_4_monday THEN 'wk_5'
      WHEN date >= wb.wk_6_monday AND date < wb.wk_5_monday THEN 'wk_6'
    END AS week_label,
    SUM(visits) AS total_visits
  FROM newsletter_daily v
  JOIN week_boundaries wb ON 1=1
  WHERE date >= wb.wk_6_monday AND date < wb.this_monday
  GROUP BY week_label
),
peak_check AS (
  SELECT
    MAX(CASE WHEN week_label = 'last_week' THEN total_visits END) AS last_week_total,
    MAX(CASE WHEN week_label != 'last_week' THEN total_visits END) AS prev_max
  FROM weekly_visits
)
SELECT
  CASE 
    WHEN last_week_total > prev_max * 1.10 THEN 'True'
    ELSE 'False'
  END AS status
FROM peak_check;
