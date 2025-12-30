
WITH date_range AS (
  SELECT
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) AS week_start,
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AS week_end
),
filtered_pages AS (
  SELECT siteid, date, postid, unique_pageviews
  FROM prod.pages
  WHERE date BETWEEN (SELECT week_start FROM date_range) AND (SELECT week_end FROM date_range)
    AND siteid = %(siteid)s
),
filtered_goals AS (
  SELECT Site_ID, Date, Min_pageviews
  FROM prod.goals
  WHERE Date BETWEEN (SELECT week_start FROM date_range) AND (SELECT week_end FROM date_range)
    AND Site_ID = %(siteid)s
),
page_goal_data AS (
  SELECT
    p.siteid,
    p.date,
    p.postid,
    p.unique_pageviews,
    g.Min_pageviews
  FROM filtered_pages p
  JOIN filtered_goals g
    ON p.siteid = g.Site_ID
   AND p.date = g.Date
),
evaluation AS (
  SELECT
    COUNT(*) AS total_articles,
    SUM(CASE WHEN CAST(p.unique_pageviews AS UNSIGNED) < CAST(p.Min_pageviews AS UNSIGNED) THEN 1 ELSE 0 END) AS below_goal_count
  FROM page_goal_data p
)
SELECT
  CASE 
    WHEN below_goal_count > total_articles / 2 THEN 'True'
    ELSE 'False'
  END AS more_than_50pct_below_goal
FROM evaluation;
