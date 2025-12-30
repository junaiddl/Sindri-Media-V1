-- -- WITH newsletter_visits AS (
-- --   SELECT 
-- --     date, 
-- --     SUM(visits) AS newsletter_total
-- --   FROM prod.newsletter
-- --   WHERE site_id = 4
-- --   GROUP BY date
-- -- ),
-- -- direct_visits AS (
-- --   SELECT 
-- --     date, 
-- --     COUNT(*) AS direct_total
-- --   FROM prod.traffic_channels
-- --   WHERE siteid = 4 AND referrer_type = 'direct'
-- --   GROUP BY date
-- -- ),
-- -- traffic_visits AS (
-- --   SELECT 
-- --     date, 
-- --     COUNT(*) AS traffic_total
-- --   FROM prod.traffic_channels
-- --   WHERE siteid = 4
-- --   GROUP BY date
-- -- ),
-- -- -- Combine newsletter and traffic visits
-- -- total_visits AS (
-- --   SELECT 
-- --     COALESCE(t.date, n.date) AS date,
-- --     COALESCE(n.newsletter_total, 0) AS newsletter_total,
-- --     COALESCE(t.traffic_total, 0) AS traffic_total,
-- --     COALESCE(n.newsletter_total, 0) + COALESCE(t.traffic_total, 0) AS total_visits
-- --   FROM traffic_visits t
-- --   LEFT JOIN newsletter_visits n ON t.date = n.date

-- --   UNION

-- --   SELECT 
-- --     n.date,
-- --     n.newsletter_total,
-- --     COALESCE(t.traffic_total, 0),
-- --     n.newsletter_total + COALESCE(t.traffic_total, 0)
-- --   FROM newsletter_visits n
-- --   LEFT JOIN traffic_visits t ON n.date = t.date
-- -- )
-- -- SELECT 
-- --   COALESCE(tv.date, d.date) AS date,
-- --   COALESCE(tv.newsletter_total, 0) AS newsletter_total,
-- --   COALESCE(d.direct_total, 0) AS direct_total,
-- --   COALESCE(tv.total_visits, 0) AS total_visits,
-- --   ROUND(100.0 * COALESCE(tv.newsletter_total, 0) / NULLIF(tv.total_visits, 0), 2) AS newsletter_pct,
-- --   ROUND(100.0 * COALESCE(d.direct_total, 0) / NULLIF(tv.total_visits, 0), 2) AS direct_pct,
-- --   -- String 'True' or 'False' for the condition
-- --   CASE 
-- --     WHEN ROUND(100.0 * (COALESCE(tv.newsletter_total, 0) + COALESCE(d.direct_total, 0)) / NULLIF(tv.total_visits, 0), 2) < 40 
-- --     THEN 'True' 
-- --     ELSE 'False' 
-- --   END AS less_than_40pct
-- -- FROM total_visits tv
-- -- LEFT JOIN direct_visits d ON tv.date = d.date

-- -- UNION

-- -- SELECT 
-- --   d.date,
-- --   0 AS newsletter_total,
-- --   d.direct_total,
-- --   0 AS total_visits,
-- --   0 AS newsletter_pct,
-- --   ROUND(100.0 * d.direct_total / NULLIF(d.direct_total, 0), 2) AS direct_pct,
-- --   'False' AS less_than_40pct
-- -- FROM direct_visits d
-- -- LEFT JOIN total_visits tv ON d.date = tv.date
-- -- WHERE tv.date IS NULL

-- -- ORDER BY date;


-- WITH last_week_dates AS (
--   SELECT 
--     DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AS last_sunday,
--     DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) AS last_monday
-- ),
-- newsletter_visits AS (
--   SELECT 
--     SUM(visits) AS newsletter_total
--   FROM prod.newsletter
--   JOIN last_week_dates d
--     ON date BETWEEN d.last_monday AND d.last_sunday
--   WHERE site_id = 4
-- ),
-- direct_visits AS (
--   SELECT 
--     COUNT(*) AS direct_total
--   FROM prod.traffic_channels
--   JOIN last_week_dates d
--     ON date BETWEEN d.last_monday AND d.last_sunday
--   WHERE siteid = 4 AND referrer_type = 'direct'
-- ),
-- traffic_visits AS (
--   SELECT 
--     COUNT(*) AS traffic_total
--   FROM prod.traffic_channels
--   JOIN last_week_dates d
--     ON date BETWEEN d.last_monday AND d.last_sunday
--   WHERE siteid = 4
-- )
-- SELECT
--   nv.newsletter_total,
--   dv.direct_total,
--   tv.traffic_total AS total_visits,
--   ROUND(100.0 * nv.newsletter_total / NULLIF(tv.traffic_total, 0), 2) AS newsletter_pct,
--   ROUND(100.0 * dv.direct_total / NULLIF(tv.traffic_total, 0), 2) AS direct_pct,
--   ROUND(100.0 * (nv.newsletter_total + dv.direct_total) / NULLIF(tv.traffic_total, 0), 2) AS combined_pct,
--   CASE 
--     WHEN (nv.newsletter_total + dv.direct_total) < 0.4 * tv.traffic_total THEN 'True'
--     ELSE 'False'
--   END AS less_than_40pct
-- FROM newsletter_visits nv
-- JOIN direct_visits dv ON 1=1
-- JOIN traffic_visits tv ON 1=1;


WITH last_week_dates AS (
  SELECT 
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AS last_sunday,
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) AS last_monday
),
newsletter_visits AS (
  SELECT 
    SUM(visits) AS newsletter_total
  FROM prod.newsletter
  JOIN last_week_dates d
    ON date BETWEEN d.last_monday AND d.last_sunday
  WHERE site_id = %(siteid)s
),
direct_visits AS (
  SELECT 
    COUNT(*) AS direct_total
  FROM prod.traffic_channels
  JOIN last_week_dates d
    ON date BETWEEN d.last_monday AND d.last_sunday
  WHERE siteid = %(siteid)s AND referrer_type = 'direct'
),
traffic_visits AS (
  SELECT 
    COUNT(*) AS traffic_total
  FROM prod.traffic_channels
  JOIN last_week_dates d
    ON date BETWEEN d.last_monday AND d.last_sunday
  WHERE siteid = %(siteid)s
)
SELECT
  CASE 
    WHEN (nv.newsletter_total + dv.direct_total) < 0.4 * tv.traffic_total THEN 'True'
    ELSE 'False'
  END AS status
FROM newsletter_visits nv
JOIN direct_visits dv ON 1=1
JOIN traffic_visits tv ON 1=1;
