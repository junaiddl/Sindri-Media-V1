-- WITH daily_actuals AS (
--   SELECT
--     e.date,
--     SUM(e.hits) AS actual_visits
--   FROM prod.events e
--   WHERE e.siteid = 4
--     AND e.date >= '2024-01-01'
--   GROUP BY e.date
-- ),
-- daily_goals AS (
--   SELECT
--     g.date,
--     g.Visits_per_day AS goal_visits
--   FROM prod.goals g
--   WHERE g.site_id = 4
--     AND g.date >= '2024-01-01'
-- )
-- SELECT
--   g.date,
--   g.goal_visits AS goal,
--   a.actual_visits,
--   ROUND(100.0 * a.actual_visits / g.goal_visits, 1) AS pct_of_goal,
--   CASE 
--     WHEN a.actual_visits < 0.85 * g.goal_visits THEN 'True'
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
--     SUM(e.hits) AS actual_visits
--   FROM prod.events e
--   JOIN last_week_dates d
--     ON e.date BETWEEN d.last_monday AND d.last_sunday
--   WHERE e.siteid = 4
-- ),
-- weekly_goals AS (
--   SELECT
--     SUM(g.Visits_per_day) AS goal_visits
--   FROM prod.goals g
--   JOIN last_week_dates d
--     ON g.date BETWEEN d.last_monday AND d.last_sunday
--   WHERE g.site_id = 4
-- )
-- SELECT
--   wg.goal_visits AS goal,
--   wa.actual_visits,
--   ROUND(100.0 * wa.actual_visits / wg.goal_visits, 1) AS pct_of_goal,
--   CASE 
--     WHEN wa.actual_visits < 0.85 * wg.goal_visits THEN 'True'
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
    SUM(e.visits) AS actual_visits
  FROM prod.daily_totals e
  JOIN last_week_dates d
    ON e.date BETWEEN d.last_monday AND d.last_sunday
  WHERE e.siteid = %(siteid)s
),
weekly_goals AS (
  SELECT
    SUM(g.Visits_per_day) AS goal_visits
  FROM prod.goals g
  JOIN last_week_dates d
    ON g.date BETWEEN d.last_monday AND d.last_sunday
  WHERE g.site_id = %(siteid)s
)
SELECT
  CASE 
    WHEN wa.actual_visits < 0.85 * wg.goal_visits THEN 'True'
    ELSE 'False'
  END AS status
FROM weekly_actuals wa
JOIN weekly_goals wg ON 1=1;

