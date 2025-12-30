-- WITH daily_visits AS (
--   SELECT
--     siteid,
--     date,
--     SUM(hits) AS visits
--   FROM prod.events
--   WHERE siteid = 4
--     AND date >= '2024-01-01'
--   GROUP BY siteid, date
-- ),
-- weekly_avg_visits AS (
--   SELECT
--     siteid,
--     DATE_SUB(date, INTERVAL WEEKDAY(date) DAY) AS week_start,
--     AVG(visits) AS avg_weekly_visits
--   FROM daily_visits
--   GROUP BY siteid, week_start
-- ),
-- ranked_weeks AS (
--   SELECT
--     siteid,
--     week_start,
--     avg_weekly_visits,
--     ROW_NUMBER() OVER (PARTITION BY siteid ORDER BY week_start DESC) AS rn
--   FROM weekly_avg_visits
-- ),
-- pivoted AS (
--   SELECT
--     siteid,
--     MAX(CASE WHEN rn = 1 THEN avg_weekly_visits END) AS week1,
--     MAX(CASE WHEN rn = 2 THEN avg_weekly_visits END) AS week2,
--     MAX(CASE WHEN rn = 3 THEN avg_weekly_visits END) AS week3
--   FROM ranked_weeks
--   WHERE rn <= 3
--   GROUP BY siteid
-- )
-- SELECT
--   siteid,
--   week3 AS Third_last_week,
--   week2 AS Second_last_week,
--   week1 AS L,
--   CASE
--   WHEN week3 > week2 AND week2 > week1 THEN 'True'
--     ELSE 'False'
--   END AS is_trending_downward
-- FROM pivoted;


WITH daily_visits AS (
  SELECT
    siteid,
    date,
    SUM(hits) AS visits
  FROM prod.events
  WHERE siteid = %(siteid)s
    AND date >= '2024-01-01'
  GROUP BY siteid, date
),
weekly_avg_visits AS (
  SELECT
    siteid,
    DATE_SUB(date, INTERVAL WEEKDAY(date) DAY) AS week_start,
    AVG(visits) AS avg_weekly_visits
  FROM daily_visits
  GROUP BY siteid, week_start
),
ranked_weeks AS (
  SELECT
    siteid,
    week_start,
    avg_weekly_visits,
    ROW_NUMBER() OVER (PARTITION BY siteid ORDER BY week_start DESC) AS rn
  FROM weekly_avg_visits
),
pivoted AS (
  SELECT
    siteid,
    MAX(CASE WHEN rn = 1 THEN avg_weekly_visits END) AS week1,
    MAX(CASE WHEN rn = 2 THEN avg_weekly_visits END) AS week2,
    MAX(CASE WHEN rn = 3 THEN avg_weekly_visits END) AS week3
  FROM ranked_weeks
  WHERE rn <= 3
  GROUP BY siteid
)
SELECT
  CASE
    WHEN week3 > week2 AND week2 > week1 THEN 'True'
    ELSE 'False'
  END AS status
FROM pivoted;
