-- WITH daily_actuals AS (
--   SELECT
--     p.date,
--     SUM(p.unique_pageviews) AS actual_pageviews
--   FROM prod.pages p
--   WHERE p.siteid = 4
--     AND p.date >= '2024-01-01'
--   GROUP BY p.date
-- ),
-- daily_goals AS (
--   SELECT
--     g.date,
--     g.Pageviews_per_day AS goal_pageviews
--   FROM prod.goals g
--   WHERE g.site_id = 4
--     AND g.date >= '2024-01-01'
-- )
-- SELECT
--   g.date,
--   g.goal_pageviews AS goal,
--   a.actual_pageviews,
--   ROUND(100.0 * a.actual_pageviews / g.goal_pageviews, 1) AS pct_of_goal,
--   CASE 
--     WHEN a.actual_pageviews < 0.85 * g.goal_pageviews THEN 'True'
--     ELSE 'False'
--   END AS below_85pct
-- FROM daily_goals g
-- JOIN daily_actuals a ON g.date = a.date
-- ORDER BY g.date;



-- WITH last_week_dates AS (
--   SELECT 
--     DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AS last_sunday,
--     DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) AS last_monday
-- ),
-- weekly_actuals AS (
--   SELECT
--     SUM(p.unique_pageviews) AS actual_pageviews
--   FROM prod.pages p
--   JOIN last_week_dates d
--     ON p.date BETWEEN d.last_monday AND d.last_sunday
--   WHERE p.siteid = 4
-- ),
-- weekly_goals AS (
--   SELECT
--     SUM(g.Pageviews_per_day) AS goal_pageviews
--   FROM prod.goals g
--   JOIN last_week_dates d
--     ON g.date BETWEEN d.last_monday AND d.last_sunday
--   WHERE g.site_id = 4
-- )
-- SELECT
--   wg.goal_pageviews AS goal,
--   wa.actual_pageviews,
--   ROUND(100.0 * wa.actual_pageviews / wg.goal_pageviews, 1) AS pct_of_goal,
--   CASE 
--     WHEN wa.actual_pageviews < 0.85 * wg.goal_pageviews THEN 'True'
--     ELSE 'False'
--   END AS below_85pct
-- FROM weekly_actuals wa
-- JOIN weekly_goals wg ON 1=1;


WITH last_week_dates AS (
  SELECT 
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AS last_sunday,
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) AS last_monday
),
weekly_actuals AS (
  SELECT
    SUM(p.unique_pageviews) AS actual_pageviews
  FROM prod.daily_totals p
  JOIN last_week_dates d
    ON p.date BETWEEN d.last_monday AND d.last_sunday
  WHERE p.siteid = %(siteid)s
),
weekly_goals AS (
  SELECT
    SUM(g.Pageviews_per_day) AS goal_pageviews
  FROM prod.goals g
  JOIN last_week_dates d
    ON g.date BETWEEN d.last_monday AND d.last_sunday
  WHERE g.site_id = %(siteid)s
)
SELECT
  CASE 
    WHEN wa.actual_pageviews < 0.85 * wg.goal_pageviews THEN 'True'
    ELSE 'False'
  END AS status
FROM weekly_actuals wa
JOIN weekly_goals wg ON 1=1;
