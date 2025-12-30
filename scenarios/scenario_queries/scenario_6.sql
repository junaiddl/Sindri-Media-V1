-- WITH daily_pageviews AS (
--   SELECT
--     siteid,
--     date,
--     SUM(unique_pageviews) AS pageviews
--   FROM prod.pages
--   WHERE siteid = 4
--     AND date >= '2024-01-01'
--   GROUP BY siteid, date
-- ),
-- daily_visits AS (
--   SELECT
--     siteid,
--     date,
--     SUM(hits) AS visits
--   FROM prod.events
--   WHERE siteid = 4
--     AND date >= '2024-01-01'
--   GROUP BY siteid, date
-- ),
-- daily_cta AS (
--   SELECT
--     pv.siteid,
--     pv.date,
--     ROUND(100.0 * v.visits / NULLIF(pv.pageviews, 0), 2) AS cta_pct
--   FROM daily_pageviews pv
--   JOIN daily_visits v ON pv.date = v.date AND pv.siteid = v.siteid
-- ),
-- weekly_avg_cta AS (
--   SELECT
--     siteid,
--     DATE_SUB(date, INTERVAL WEEKDAY(date) DAY) AS week_start,
--     AVG(cta_pct) AS avg_weekly_cta
--   FROM daily_cta
--   GROUP BY siteid, week_start
-- ),
-- ranked_weeks AS (
--   SELECT
--     siteid,
--     week_start,
--     avg_weekly_cta,
--     ROW_NUMBER() OVER (PARTITION BY siteid ORDER BY week_start DESC) AS rn
--   FROM weekly_avg_cta
-- ),
-- pivoted AS (
--   SELECT
--     siteid,
--     MAX(CASE WHEN rn = 1 THEN avg_weekly_cta END) AS week1,
--     MAX(CASE WHEN rn = 2 THEN avg_weekly_cta END) AS week2,
--     MAX(CASE WHEN rn = 3 THEN avg_weekly_cta END) AS week3
--   FROM ranked_weeks
--   WHERE rn <= 3
--   GROUP BY siteid
-- )
-- SELECT
--   siteid,
--   week3 AS Third_last_week,
--   week2 AS Second_last_week,
--   week1 AS Last_week,
--   CASE
--     WHEN week3 > week2 AND week2 > week1 THEN 'True'
--     ELSE 'False'
--   END AS is_trending_downward
-- FROM pivoted;

WITH daily_pageviews AS (
  SELECT
    siteid,
    date,
    SUM(unique_pageviews) AS pageviews
  FROM prod.pages
  WHERE siteid = %(siteid)s
    AND date >= '2024-01-01'
  GROUP BY siteid, date
),
daily_visits AS (
  SELECT
    siteid,
    date,
    SUM(hits) AS visits
  FROM prod.events
  WHERE siteid = %(siteid)s
    AND date >= '2024-01-01'
  GROUP BY siteid, date
),
daily_cta AS (
  SELECT
    pv.siteid,
    pv.date,
    ROUND(100.0 * v.visits / NULLIF(pv.pageviews, 0), 2) AS cta_pct
  FROM daily_pageviews pv
  JOIN daily_visits v ON pv.date = v.date AND pv.siteid = v.siteid
),
weekly_avg_cta AS (
  SELECT
    siteid,
    DATE_SUB(date, INTERVAL WEEKDAY(date) DAY) AS week_start,
    AVG(cta_pct) AS avg_weekly_cta
  FROM daily_cta
  GROUP BY siteid, week_start
),
ranked_weeks AS (
  SELECT
    siteid,
    week_start,
    avg_weekly_cta,
    ROW_NUMBER() OVER (PARTITION BY siteid ORDER BY week_start DESC) AS rn
  FROM weekly_avg_cta
),
pivoted AS (
  SELECT
    siteid,
    MAX(CASE WHEN rn = 1 THEN avg_weekly_cta END) AS week1,
    MAX(CASE WHEN rn = 2 THEN avg_weekly_cta END) AS week2,
    MAX(CASE WHEN rn = 3 THEN avg_weekly_cta END) AS week3
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
