-- WITH last_week_dates AS (
--   SELECT 
--     CURDATE() - INTERVAL (WEEKDAY(CURDATE()) + 1) DAY AS end_date,  -- Last Sunday
--     CURDATE() - INTERVAL (WEEKDAY(CURDATE()) + 7) DAY AS start_date  -- Last Monday
-- ),
-- filtered_articles AS (
--   SELECT id AS postid
--   FROM prod.site_archive_post
--   WHERE userneeds = 'Underhold mig'
-- ),
-- weekly_page_data AS (
--   SELECT 
--     p.siteid,
--     p.date,
--     p.postid,
--     p.unique_pageviews,
--     g.Min_pageviews
--   FROM prod.pages p
--   JOIN prod.goals g
--     ON p.siteid = g.Site_ID AND p.date = g.Date
--   JOIN filtered_articles a
--     ON p.postid = a.postid
--   JOIN last_week_dates w
--     ON p.date BETWEEN w.start_date AND w.end_date
-- ),
-- evaluation AS (
--   SELECT
--     COUNT(*) AS total_articles,
--     SUM(CASE 
--           WHEN CAST(p.unique_pageviews AS UNSIGNED) < CAST(p.Min_pageviews AS UNSIGNED) 
--           THEN 1 ELSE 0 
--         END) AS below_goal_count
--   FROM weekly_page_data p
-- )
-- SELECT
--   total_articles,
--   below_goal_count,
--   ROUND(100.0 * below_goal_count / total_articles, 2) AS below_goal_pct,
--   CASE 
--     WHEN below_goal_count > total_articles * 0.55 THEN 'True'
--     ELSE 'False'
--   END AS more_than_55pct_below_goal
-- FROM evaluation;


WITH last_week_dates AS (
  SELECT 
    CURDATE() - INTERVAL (WEEKDAY(CURDATE()) + 1) DAY AS end_date,  -- Last Sunday
    CURDATE() - INTERVAL (WEEKDAY(CURDATE()) + 7) DAY AS start_date  -- Last Monday
),
filtered_articles AS (
  SELECT id AS postid
  FROM prod.site_archive_post
  WHERE userneeds = 'Underhold mig'
  and siteid = %(siteid)s
),
weekly_page_data AS (
  SELECT 
    p.siteid,
    p.date,
    p.postid,
    p.unique_pageviews,
    g.Min_pageviews
  FROM prod.pages p
  JOIN prod.goals g
    ON p.siteid = g.Site_ID AND p.date = g.Date
  JOIN filtered_articles a
    ON p.postid = a.postid
  JOIN last_week_dates w
    ON p.date BETWEEN w.start_date AND w.end_date
  WHERE p.siteid = %(siteid)s and g.Site_ID = %(siteid)s
),
evaluation AS (
  SELECT
    COUNT(*) AS total_articles,
    SUM(CASE 
          WHEN CAST(p.unique_pageviews AS UNSIGNED) < CAST(p.Min_pageviews AS UNSIGNED) 
          THEN 1 ELSE 0 
        END) AS below_goal_count
  FROM weekly_page_data p
)
SELECT
  CASE 
    WHEN below_goal_count > total_articles * 0.55 THEN 'True'
    ELSE 'False'
  END AS status
FROM evaluation;
