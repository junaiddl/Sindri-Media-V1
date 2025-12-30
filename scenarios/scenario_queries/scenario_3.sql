-- WITH daily_pageviews AS (
--   SELECT
--     date,
--     SUM(unique_pageviews) AS pageviews
--   FROM prod.pages
--   WHERE siteid = 4
--     AND date >= '2024-01-01'
--   GROUP BY date
-- ),
-- daily_visits AS (
--   SELECT
--     date,
--     SUM(hits) AS visits
--   FROM prod.events
--   WHERE siteid = 4
--     AND date >= '2024-01-01'
--   GROUP BY date
-- ),
-- daily_cta AS (
--   SELECT
--     pv.date,
--     ROUND(100.0 * v.visits / pv.pageviews, 2) AS actual_cta_pct
--   FROM daily_pageviews pv
--   JOIN daily_visits v ON pv.date = v.date
-- ),
-- goal_cta AS (
--   SELECT
--     date,
--     CTA_per_day AS goal_cta_pct
--   FROM prod.goals
--   WHERE site_id = 4
--     AND date >= '2024-01-01'
-- )
-- SELECT
--   c.date,
--   c.actual_cta_pct,
--   g.goal_cta_pct,
--   CASE
--     WHEN c.actual_cta_pct < 0.85 * g.goal_cta_pct THEN 'True'
--     ELSE 'False'
--   END AS below_85pct_of_goal
-- FROM daily_cta c
-- JOIN goal_cta g ON c.date = g.date
-- ORDER BY c.date;


-- WITH last_week_dates AS (
--   SELECT 
--     DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AS last_sunday,
--     DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) AS last_monday
-- ),
-- weekly_pageviews AS (
--   SELECT
--     SUM(unique_pageviews) AS pageviews
--   FROM prod.pages
--   JOIN last_week_dates d
--     ON date BETWEEN d.last_monday AND d.last_sunday
--   WHERE siteid = 4
-- ),
-- weekly_visits AS (
--   SELECT
--     SUM(hits) AS visits
--   FROM prod.events
--   JOIN last_week_dates d
--     ON date BETWEEN d.last_monday AND d.last_sunday
--   WHERE siteid = 4
-- ),
-- weekly_actual_cta AS (
--   SELECT
--     ROUND(100.0 * wv.visits / wp.pageviews, 2) AS actual_cta_pct
--   FROM weekly_pageviews wp
--   JOIN weekly_visits wv ON 1=1
-- ),
-- weekly_goal_cta AS (
--   SELECT
--     SUM(CTA_per_day) AS goal_cta_pct
--   FROM prod.goals
--   JOIN last_week_dates d
--     ON date BETWEEN d.last_monday AND d.last_sunday
--   WHERE site_id = 4
-- )
-- SELECT
--   wa.actual_cta_pct,
--   wg.goal_cta_pct,
--   CASE
--     WHEN wa.actual_cta_pct < 0.85 * wg.goal_cta_pct THEN 'True'
--     ELSE 'False'
--   END AS below_85pct_of_goal
-- FROM weekly_actual_cta wa
-- JOIN weekly_goal_cta wg ON 1=1;


WITH last_week_dates AS (
  SELECT 
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AS last_sunday,
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) AS last_monday
),
weekly_pageviews AS (
  SELECT
    SUM(unique_pageviews) AS pageviews
  FROM prod.pages
  JOIN last_week_dates d
    ON date BETWEEN d.last_monday AND d.last_sunday
  WHERE siteid = %(siteid)s
),
weekly_next_clicks AS (
  SELECT
  sum(hits) AS next_clicks
  FROM prod.events
  JOIN last_week_dates d
    ON date BETWEEN d.last_monday AND d.last_sunday
  WHERE siteid = %(siteid)s
    AND event_action = 'next click'
),
weekly_actual_cta AS (
  SELECT
    ROUND(wnc.next_clicks/wp.pageviews*100, 2) AS actual_cta_pct
  FROM weekly_pageviews wp
  JOIN weekly_next_clicks wnc ON 1=1
),
weekly_goal_cta AS (
  SELECT
    avg(CTA_per_day) AS goal_cta_pct
  FROM prod.goals
  JOIN last_week_dates d
    ON date BETWEEN d.last_monday AND d.last_sunday
  WHERE site_id = %(siteid)s
)
SELECT
  CASE
    WHEN wa.actual_cta_pct < 0.85 * wg.goal_cta_pct THEN 'True'
    ELSE 'False'
  END AS status
FROM weekly_actual_cta wa
JOIN weekly_goal_cta wg ON 1=1;
