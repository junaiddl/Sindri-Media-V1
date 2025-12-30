
WITH last_week_dates AS (
  SELECT 
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AS last_sunday,
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) AS last_monday
),
filtered_pages AS (
  SELECT p.siteid, p.date, p.postid, p.unique_pageviews
  FROM prod.pages p
  JOIN last_week_dates d
    ON p.date BETWEEN d.last_monday AND d.last_sunday
 WHERE p.siteid =  %(siteid)s
),
filtered_events AS (
  SELECT siteid, date, postid, COUNT(*) AS next_clicks
  FROM prod.events
  WHERE event_action = 'next_click' AND siteid =  %(siteid)s  
  GROUP BY siteid, date, postid
),
page_event_data AS (
  SELECT 
    p.siteid,
    p.date,
    p.postid,
    p.unique_pageviews,
    COALESCE(e.next_clicks, 0) AS next_clicks
  FROM filtered_pages p
  LEFT JOIN filtered_events e
    ON p.siteid = e.siteid AND p.date = e.date AND p.postid = e.postid
),
cta_data AS (
  SELECT 
    d.siteid,
    d.date,
    d.postid,
    d.unique_pageviews,
    d.next_clicks,
    CASE 
      WHEN d.next_clicks = 0 THEN NULL
      ELSE d.unique_pageviews / d.next_clicks
    END AS actual_cta
  FROM page_event_data d
),
joined_data AS (
  SELECT 
    c.siteid,
    c.date,
    c.postid,
    c.unique_pageviews,
    c.actual_cta,
    g.Min_CTA,
    g.Min_pageviews
  FROM cta_data c
  JOIN prod.goals g
    ON c.siteid = g.Site_ID AND c.date = g.Date
  WHERE g.Site_ID =  %(siteid)s
),
evaluation AS (
  SELECT
    COUNT(*) AS total_articles,
    SUM(CASE 
          WHEN actual_cta IS NOT NULL 
               AND actual_cta < CAST(Min_CTA AS DECIMAL(10,4)) 
               AND CAST(unique_pageviews AS UNSIGNED) < CAST(Min_pageviews AS UNSIGNED)
          THEN 1 ELSE 0 
        END) AS below_both_goals_count
  FROM joined_data
)
SELECT
  CASE 
    WHEN below_both_goals_count > total_articles / 2 THEN 'True'
    ELSE 'False'
  END AS more_than_50pct_below_both_goals
FROM evaluation;
