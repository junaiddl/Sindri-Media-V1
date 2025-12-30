def updated_queries(siteid,event):
    special_condition = ""
    if siteid == 14:
        special_condition = "AND categories <> 'Nyhedsoverblik'"
    if siteid == 11:
        special_condition = """and(
      NOT (
    tags IN (
      'Billedkunst', 'Børnehaveklassen', 'DSA', 'Engelsk', 'Håndværk og design',
      'Idræt', 'Kulturfag', 'Lærersenior', 'Lærerstuderende', 'Madkundskab',
      'Musik', 'Naturfag', 'PLC', 'PPR', 'Praktik', 'SOSU', 'Tysk og fransk', 'UU'
    )
    OR categories = 'Seneste nyt'
  )
"""

    scenario_7 = f"""
        WITH last_week_range AS (
    SELECT 
        DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) AS last_monday,
        DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AS last_sunday,
        DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 14 DAY) AS prev_monday,
        DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 8 DAY) AS prev_sunday
),


last_week_articles AS (
    SELECT 
        sap.id AS post_id,
        sap.link AS url,
        sap.title,
        sap.date AS pub_date,
        sap.userneeds,
        sap.tags,
        sap.categories,
        COALESCE(p.unique_pageviews, 0) AS unique_pageviews
    FROM prod.site_archive_post sap
    LEFT JOIN prod.pages p 
        ON sap.id = p.postid AND sap.siteid = p.siteid
        AND p.date BETWEEN (SELECT last_monday FROM last_week_range) AND (SELECT last_sunday FROM last_week_range)
    WHERE sap.date BETWEEN (SELECT last_monday FROM last_week_range) AND (SELECT last_sunday FROM last_week_range)
      AND sap.siteid = {siteid}
      {"AND  sap.categories <> 'Nyhedsoverblik'" if siteid == 14 else ""}
{"AND ( \
    NOT ( \
        sap.tags IN ( \
            'Billedkunst', 'Børnehaveklassen', 'DSA', 'Engelsk', 'Håndværk og design', \
            'Idræt', 'Kulturfag', 'Lærersenior', 'Lærerstuderende', 'Madkundskab', \
            'Musik', 'Naturfag', 'PLC', 'PPR', 'Praktik', 'SOSU', 'Tysk og fransk', 'UU' \
        ) \
        OR sap.categories = 'Seneste nyt' \
    ) \
)" if siteid == 11 else ""}
),
top_newsletter AS (
    SELECT 
        sap.id AS post_id,
        sap.link AS url,
        sap.title,
        sap.date AS pub_date,
        sap.userneeds,
        sap.tags,
        sap.categories,
        SUM(n.visits) AS newsletter_visits
    FROM prod.newsletter n
    JOIN prod.site_archive_post sap 
        ON sap.id = n.post_id AND sap.siteid = n.site_id
    WHERE n.date BETWEEN (SELECT last_monday FROM last_week_range) AND (SELECT last_sunday FROM last_week_range)
      AND n.site_id = {siteid}
          {"AND  sap.categories <> 'Nyhedsoverblik'" if siteid == 14 else ""}
{"AND ( \
    NOT ( \
        sap.tags IN ( \
            'Billedkunst', 'Børnehaveklassen', 'DSA', 'Engelsk', 'Håndværk og design', \
            'Idræt', 'Kulturfag', 'Lærersenior', 'Lærerstuderende', 'Madkundskab', \
            'Musik', 'Naturfag', 'PLC', 'PPR', 'Praktik', 'SOSU', 'Tysk og fransk', 'UU' \
        ) \
        OR sap.categories = 'Seneste nyt' \
    ) \
)" if siteid == 11 else ""}
    GROUP BY sap.id, sap.link, sap.title, sap.date, sap.userneeds, sap.tags, sap.categories
    ORDER BY newsletter_visits DESC
    LIMIT 10
),


-- 3. Top 10 Forside click articles
top_forside AS (
    SELECT 
        sap.id AS post_id,
        sap.link AS url,
        sap.title,
        sap.date AS pub_date,
        sap.userneeds,
        sap.tags,
        sap.categories,
        SUM(hits) AS front_page_clicks          -- MODIFIED FROM COUNT(*) TO SUM(hits)
    FROM prod.events e
    JOIN prod.site_archive_post sap 
        ON sap.id = e.postid AND sap.siteid = e.siteid
    WHERE e.event_action = 'Frontpage'
      AND e.date BETWEEN (SELECT last_monday FROM last_week_range) AND (SELECT last_sunday FROM last_week_range)
      AND e.siteid = {siteid}
          {"AND  sap.categories <> 'Nyhedsoverblik'" if siteid == 14 else ""}
{"AND ( \
    NOT ( \
        sap.tags IN ( \
            'Billedkunst', 'Børnehaveklassen', 'DSA', 'Engelsk', 'Håndværk og design', \
            'Idræt', 'Kulturfag', 'Lærersenior', 'Lærerstuderende', 'Madkundskab', \
            'Musik', 'Naturfag', 'PLC', 'PPR', 'Praktik', 'SOSU', 'Tysk og fransk', 'UU' \
        ) \
        OR sap.categories = 'Seneste nyt' \
    ) \
)" if siteid == 11 else ""}
    GROUP BY sap.id, sap.link, sap.title, sap.date, sap.userneeds, sap.tags, sap.categories
    ORDER BY front_page_clicks DESC
    LIMIT 10
),

-- 4. Visit Summary: Use logic from table2
traffic_mapped AS (
    SELECT
        CASE
            WHEN tc.realreferrer IN ('Direct', 'Search', 'Facebook') THEN tc.realreferrer
            WHEN tc.realreferrer = 'Newsletter' OR tc.realreferrer LIKE 'NL%' THEN 'Newsletter'
            ELSE 'Others'
        END AS mapped_referrer,
        tc.date
    FROM prod.traffic_channels tc
    WHERE tc.siteid = {siteid}
),

total_counts AS (
    SELECT SUM(weekly_count) AS total_count
    FROM (
        SELECT mapped_referrer, COUNT(*) AS weekly_count
        FROM traffic_mapped, last_week_range
        WHERE date BETWEEN last_monday AND last_sunday
        GROUP BY mapped_referrer
    ) AS sub
),

last_week_counts AS (
    SELECT mapped_referrer AS referrer, COUNT(*) AS weekly_count
    FROM traffic_mapped, last_week_range
    WHERE date BETWEEN last_monday AND last_sunday
    GROUP BY mapped_referrer
),

previous_week_counts AS (
    SELECT mapped_referrer AS referrer, COUNT(*) AS prev_weekly_count
    FROM traffic_mapped, last_week_range
    WHERE date BETWEEN prev_monday AND prev_sunday
    GROUP BY mapped_referrer
),

visit_summary AS (
    SELECT 
        l.referrer,
        tc.total_count,
        COALESCE(l.weekly_count, 0) AS weekly_count,
        COALESCE(p.prev_weekly_count, 0) AS prev_weekly_count
    FROM last_week_counts l
    LEFT JOIN previous_week_counts p ON l.referrer = p.referrer
    CROSS JOIN total_counts tc
)

-- Final Output
SELECT JSON_OBJECT(
    'last_week_articles', (
        SELECT JSON_ARRAYAGG(JSON_OBJECT(
            'post_id', COALESCE(post_id,""),
            'pub_date', COALESCE(pub_date,""),
            'userneeds', COALESCE(userneeds,""),
            'tags', COALESCE(tags,""),
            'categories', COALESCE(categories,""),
            'unique_pageviews', COALESCE(unique_pageviews,0)
        )) FROM last_week_articles
    ),
    'top_newsletter', (
        SELECT JSON_ARRAYAGG(JSON_OBJECT(
            'post_id', COALESCE(post_id,""),
            'pub_date', COALESCE(pub_date,""),
            'userneeds', COALESCE(userneeds,""),
            'tags', COALESCE(tags,""),
            'categories', COALESCE(categories,""),
            'newsletter_visits', COALESCE(newsletter_visits,0)
        )) FROM top_newsletter
    ),
    'top_forside', (
        SELECT JSON_ARRAYAGG(JSON_OBJECT(
            'post_id', COALESCE(post_id,""),
            'pub_date', COALESCE(pub_date,""),
            'userneeds', COALESCE(userneeds,""),
            'tags', COALESCE(tags,""),
            'categories', COALESCE(categories,""),
            'front_page_clicks', COALESCE(front_page_clicks,0)
        )) FROM top_forside
    ),
    'visit_summary', (
        SELECT JSON_ARRAYAGG(
            JSON_OBJECT(
                'kanal', COALESCE(referrer,""),
                'besog_uge', COALESCE(weekly_count,0),
                'andel_af_total', CONCAT(
                   COALESCE( ROUND(
                        CASE 
                            WHEN total_count = 0 THEN 0
                            ELSE (weekly_count / total_count) * 100
                        END, 1
                    ),0), '%'
                ),
                'udvikling_ift_forrige_uge', CONCAT(
                    CASE 
                        WHEN prev_weekly_count = 0 THEN '0.0'
                        WHEN (weekly_count - prev_weekly_count) / prev_weekly_count >= 0 THEN '+'
                        ELSE '-'
                    END,
                   COALESCE( ROUND(
                        CASE 
                            WHEN prev_weekly_count = 0 THEN 0
                            ELSE ABS((weekly_count - prev_weekly_count) / prev_weekly_count) * 100
                        END, 1
                    ),0), '%'
                )
            )
        )
        FROM visit_summary
        WHERE referrer IN ('Newsletter', 'Direct')
    )
) AS scenario7_output;
    """

    scenario_8 = f"""

WITH weeks AS (
  SELECT 
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 + 7 * n DAY) AS week_end,
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 + 7 * n DAY) AS week_start,
    n AS week_number
  FROM (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2) AS x
),

-- Compute visits per post per week + enrich with post data
weekly_visits AS (
  SELECT 
    w.week_number,
    w.week_start,
    w.week_end,
    n.post_id,
    MAX(p.Title) AS title,
    MAX(p.Date) AS pub_date,
    MAX(p.Link) AS article_url,
    MAX(p.Categories) AS categories,
    MAX(p.Tags) AS tags,
    MAX(p.userneeds) AS userneeds,
    SUM(n.visits) AS total_visits
  FROM prod.newsletter n
  JOIN prod.site_archive_post p ON n.post_id = p.id AND n.site_id = p.siteid
  JOIN weeks w ON n.date BETWEEN w.week_start AND w.week_end
  WHERE n.post_id IS NOT NULL AND n.site_id ={siteid}
      {"AND  p.categories <> 'Nyhedsoverblik'" if siteid == 14 else ""}
{"AND ( \
    NOT ( \
        p.tags IN ( \
            'Billedkunst', 'Børnehaveklassen', 'DSA', 'Engelsk', 'Håndværk og design', \
            'Idræt', 'Kulturfag', 'Lærersenior', 'Lærerstuderende', 'Madkundskab', \
            'Musik', 'Naturfag', 'PLC', 'PPR', 'Praktik', 'SOSU', 'Tysk og fransk', 'UU' \
        ) \
        OR p.categories = 'Seneste nyt' \
    ) \
)" if siteid == 11 else ""}
  GROUP BY w.week_number, w.week_start, w.week_end, n.post_id
),

-- Rank top articles
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY week_number ORDER BY total_visits DESC) AS rnk
  FROM weekly_visits
),

-- Top 10 articles per week with post info
json_per_week AS (
  SELECT 
    week_number,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'post_id', COALESCE(post_id, ""),
        'pub_date', COALESCE(pub_date, ""),
        'total_visits', COALESCE(total_visits, 0),
        'categories', COALESCE(categories, ""),
        'tags', COALESCE(tags, ""),
        'userneeds', COALESCE(userneeds, "")
      )
    ) AS top_articles
  FROM ranked
  WHERE rnk <= 10
  GROUP BY week_number
),

-- Identify most recent week where visits increased vs previous week
weekly_totals_with_growth AS (
  SELECT 
    YEARWEEK(date, 3) AS year_week,
    MIN(date) AS week_start,
    MAX(date) AS week_end,
    SUM(visits) AS total_visits,
    LAG(SUM(visits)) OVER (ORDER BY YEARWEEK(date, 3)) AS prev_week_visits
  FROM prod.newsletter
  WHERE post_id IS NOT NULL AND site_id ={siteid}
  GROUP BY YEARWEEK(date, 3)
),

week_with_max_visits AS (
  SELECT week_start, week_end
  FROM weekly_totals_with_growth
  WHERE total_visits > prev_week_visits
  ORDER BY week_start DESC
  LIMIT 1
),

-- Top 10 articles in peak week (latest visit growth week), enriched
peak_week_top_articles AS (
  SELECT 
    n.post_id,
    MAX(p.Title) AS title,
    MAX(p.Date) AS pub_date,
    MAX(p.Link) AS article_url,
    MAX(p.Categories) AS categories,
    MAX(p.Tags) AS tags,
    MAX(p.userneeds) AS userneeds,
    SUM(n.visits) AS total_visits
  FROM prod.newsletter n
  JOIN week_with_max_visits w ON n.date BETWEEN w.week_start AND w.week_end
  JOIN prod.site_archive_post p ON n.post_id = p.id AND n.site_id = p.siteid
  WHERE n.post_id IS NOT NULL AND n.site_id ={siteid}
      {"AND  p.categories <> 'Nyhedsoverblik'" if siteid == 14 else ""}
{"AND ( \
    NOT ( \
        p.tags IN ( \
            'Billedkunst', 'Børnehaveklassen', 'DSA', 'Engelsk', 'Håndværk og design', \
            'Idræt', 'Kulturfag', 'Lærersenior', 'Lærerstuderende', 'Madkundskab', \
            'Musik', 'Naturfag', 'PLC', 'PPR', 'Praktik', 'SOSU', 'Tysk og fransk', 'UU' \
        ) \
        OR p.categories = 'Seneste nyt' \
    ) \
)" if siteid == 11 else ""}
  GROUP BY n.post_id
  ORDER BY total_visits DESC
  LIMIT 10
),

-- Weekly totals and grand total for last 3 weeks
weekly_shares_with_total AS (
  SELECT 
    CONCAT(DATE_FORMAT(w.week_start, '%Y-%m-%d'), ' to ', DATE_FORMAT(w.week_end, '%Y-%m-%d')) AS week,
    count(*) AS total_visits,
    (
      SELECT count(*)
      FROM prod.traffic_channels n2
      JOIN weeks w2 ON n2.date BETWEEN w2.week_start AND w2.week_end
      WHERE n2.siteid ={siteid} and n2.realreferrer like 'NL%'
    ) AS grand_total
  FROM prod.traffic_channels n
  JOIN weeks w ON n.date BETWEEN w.week_start AND w.week_end
  WHERE n.siteid ={siteid} and n.realreferrer like 'NL%'
  GROUP BY w.week_number, w.week_start, w.week_end
),

-- Share % JSON
weekly_share_json AS (
  SELECT 
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'week', COALESCE(week, ""),
        'total_visits', COALESCE(total_visits, 0),
        'share_pct', ROUND(COALESCE(total_visits, 0) / COALESCE(grand_total, 1) * 100, 1)
      )
    ) AS share_summary
  FROM weekly_shares_with_total
)

-- Final JSON Output
SELECT JSON_OBJECT(
  'last_week',      (SELECT top_articles FROM json_per_week WHERE week_number = 0),
  'previous_week',  (SELECT top_articles FROM json_per_week WHERE week_number = 1),
  'week_before',    (SELECT top_articles FROM json_per_week WHERE week_number = 2),
  'peak_week',      (SELECT JSON_ARRAYAGG(
                        JSON_OBJECT(
                          'post_id', COALESCE(post_id, ""),
                          'pub_date', COALESCE(pub_date, ""),
                          'total_visits', COALESCE(total_visits, 0),
                          'categories', COALESCE(categories, ""),
                          'tags', COALESCE(tags, ""),
                          'userneeds', COALESCE(userneeds, "")
                        )
                      ) FROM peak_week_top_articles),
  'weekly_share_summary', (SELECT share_summary FROM weekly_share_json)
) AS newsletter_insights;

"""
    scenario_9 = f""" 

WITH weeks AS (
  SELECT 
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 + 7 * n DAY) AS week_end,
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 + 7 * n DAY) AS week_start,
    YEARWEEK(DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 + 7 * n DAY), 3) AS year_week,
    n AS week_number
  FROM (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2) AS x
),

weekly_totals AS (
  SELECT 
    w.year_week,
    w.week_start,
    w.week_end,
    SUM(n.visits) AS total_visits
  FROM prod.newsletter n
  JOIN weeks w ON n.date BETWEEN w.week_start AND w.week_end
  WHERE n.post_id IS NOT NULL AND n.site_id = {siteid}
  GROUP BY w.year_week, w.week_start, w.week_end
),

-- Detect the most recent week where visits fell from the week before
latest_week_with_drop AS (
  SELECT 
    curr.year_week,
    curr.week_start,
    curr.week_end,
    curr.total_visits,
    prev.total_visits AS prev_total_visits
  FROM weekly_totals curr
  JOIN weekly_totals prev ON prev.week_end = DATE_SUB(curr.week_start, INTERVAL 1 DAY)
  WHERE curr.total_visits < prev.total_visits
  ORDER BY curr.week_start DESC
  LIMIT 1
),

-- Low week top 10 articles
low_week_top_articles AS (
  SELECT 
    n.post_id,
    MAX(p.Title) AS title,
    MAX(p.Date) AS pub_date,
    MAX(p.Link) AS article_url,
    MAX(p.Categories) AS categories,
    MAX(p.Tags) AS tags,
    MAX(p.userneeds) AS userneeds,
    SUM(n.visits) AS total_visits
  FROM prod.newsletter n
  JOIN latest_week_with_drop w ON n.date BETWEEN w.week_start AND w.week_end
  JOIN prod.site_archive_post p ON n.post_id = p.id AND n.site_id = p.siteid
  WHERE n.post_id IS NOT NULL AND n.site_id = {siteid}
      {"AND  p.categories <> 'Nyhedsoverblik'" if siteid == 14 else ""}
{"AND ( \
    NOT ( \
        p.tags IN ( \
            'Billedkunst', 'Børnehaveklassen', 'DSA', 'Engelsk', 'Håndværk og design', \
            'Idræt', 'Kulturfag', 'Lærersenior', 'Lærerstuderende', 'Madkundskab', \
            'Musik', 'Naturfag', 'PLC', 'PPR', 'Praktik', 'SOSU', 'Tysk og fransk', 'UU' \
        ) \
        OR p.categories = 'Seneste nyt' \
    ) \
)" if siteid == 11 else ""}
  GROUP BY n.post_id
  ORDER BY total_visits DESC
  LIMIT 10
),

-- Weekly post-level visits
weekly_visits AS (
  SELECT 
    w.week_number,
    w.week_start,
    w.week_end,
    n.post_id,
    MAX(p.Title) AS title,
    MAX(p.Date) AS pub_date,
    MAX(p.Link) AS article_url,
    MAX(p.Categories) AS categories,
    MAX(p.Tags) AS tags,
    MAX(p.userneeds) AS userneeds,
    SUM(n.visits) AS total_visits
  FROM prod.newsletter n
  JOIN prod.site_archive_post p ON n.post_id = p.id AND n.site_id = p.siteid
  JOIN weeks w ON n.date BETWEEN w.week_start AND w.week_end
  WHERE n.post_id IS NOT NULL AND n.site_id = {siteid}
      {"AND  p.categories <> 'Nyhedsoverblik'" if siteid == 14 else ""}
{"AND ( \
    NOT ( \
        p.tags IN ( \
            'Billedkunst', 'Børnehaveklassen', 'DSA', 'Engelsk', 'Håndværk og design', \
            'Idræt', 'Kulturfag', 'Lærersenior', 'Lærerstuderende', 'Madkundskab', \
            'Musik', 'Naturfag', 'PLC', 'PPR', 'Praktik', 'SOSU', 'Tysk og fransk', 'UU' \
        ) \
        OR p.categories = 'Seneste nyt' \
    ) \
)" if siteid == 11 else ""}
  GROUP BY w.week_number, w.week_start, w.week_end, n.post_id
),

ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY week_number ORDER BY total_visits DESC) AS rnk
  FROM weekly_visits
),

json_per_week AS (
  SELECT 
    week_number,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'post_id', COALESCE(post_id, ""),
        'pub_date', COALESCE(pub_date, ""),
        'total_visits', COALESCE(total_visits, 0),
        'categories', COALESCE(categories, ""),
        'tags', COALESCE(tags, ""),
        'userneeds', COALESCE(userneeds, "")
      )
    ) AS top_articles
  FROM ranked
  WHERE rnk <= 10
  GROUP BY week_number
),

weekly_shares_with_total AS (
  SELECT 
    CONCAT(DATE_FORMAT(w.week_start, '%Y-%m-%d'), ' to ', DATE_FORMAT(w.week_end, '%Y-%m-%d')) AS week,
    count(*) AS total_visits,
    (
      SELECT count(*)
      FROM prod.traffic_channels n2
      JOIN weeks w2 ON n2.date BETWEEN w2.week_start AND w2.week_end
      WHERE n2.siteid = {siteid} and n2.realreferrer like 'NL%'
    ) AS grand_total
  FROM prod.traffic_channels n 
  JOIN weeks w ON n.date BETWEEN w.week_start AND w.week_end
  WHERE n.siteid = {siteid} and n.realreferrer like 'NL%'
  GROUP BY w.week_number, w.week_start, w.week_end
),

weekly_share_json AS (
  SELECT 
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'week', COALESCE(week, ""),
        'total_visits', COALESCE(total_visits, 0),
        'share_pct', ROUND(COALESCE(total_visits, 0) / COALESCE(grand_total, 1) * 100, 1)
      )
    ) AS share_summary
  FROM weekly_shares_with_total
)

-- Final Output
SELECT JSON_OBJECT(
  'last_week',      (SELECT top_articles FROM json_per_week WHERE week_number = 0),
  'previous_week',  (SELECT top_articles FROM json_per_week WHERE week_number = 1),
  'week_before',    (SELECT top_articles FROM json_per_week WHERE week_number = 2),
  'low_week',       (SELECT JSON_ARRAYAGG(
                       JSON_OBJECT(
                         'post_id', COALESCE(post_id, ""),
                         'pub_date', COALESCE(pub_date, ""),
                         'total_visits', COALESCE(total_visits, 0),
                         'categories', COALESCE(categories, ""),
                         'tags', COALESCE(tags, ""),
                         'userneeds', COALESCE(userneeds, "")
                       )
                     ) FROM low_week_top_articles),
  'weekly_share_summary', (SELECT share_summary FROM weekly_share_json)
) AS newsletter_insights;
"""
    scenario_10 = f"""
WITH weeks AS (
  SELECT 
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 + 7 * n DAY) AS week_end,
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 + 7 * n DAY) AS week_start,
    n AS week_number
  FROM (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2) AS x
),

-- Aggregate weekly visits for last 3 weeks
weekly_visits AS (
  SELECT 
    w.week_number,
    w.week_start,
    w.week_end,
    n.post_id,
    MAX(p.Title) AS title,
    MAX(p.Date) AS pub_date,
    MAX(p.Link) AS article_url,
    MAX(p.Categories) AS categories,
    MAX(p.Tags) AS tags,
    MAX(p.userneeds) AS userneeds,
    SUM(n.visits) AS total_visits
  FROM prod.newsletter n
  JOIN prod.site_archive_post p ON n.post_id = p.id AND n.site_id = p.siteid
  JOIN weeks w ON n.date BETWEEN w.week_start AND w.week_end
  WHERE n.post_id IS NOT NULL AND n.site_id = {siteid}
      {"AND  p.categories <> 'Nyhedsoverblik'" if siteid == 14 else ""}
{"AND ( \
    NOT ( \
        p.tags IN ( \
            'Billedkunst', 'Børnehaveklassen', 'DSA', 'Engelsk', 'Håndværk og design', \
            'Idræt', 'Kulturfag', 'Lærersenior', 'Lærerstuderende', 'Madkundskab', \
            'Musik', 'Naturfag', 'PLC', 'PPR', 'Praktik', 'SOSU', 'Tysk og fransk', 'UU' \
        ) \
        OR p.categories = 'Seneste nyt' \
    ) \
)" if siteid == 11 else ""}
  GROUP BY w.week_number, w.week_start, w.week_end, n.post_id
),

-- Rank articles per week
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY week_number ORDER BY total_visits DESC) AS rnk
  FROM weekly_visits
),

-- Get top 10 per recent week
json_per_week AS (
  SELECT 
    week_number,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'post_id', COALESCE(post_id,""),
        'pub_date', COALESCE(pub_date,""),
        'total_visits', COALESCE(total_visits,0),
        'categories', COALESCE(categories,""),
        'tags', COALESCE(tags,""),
       
        'userneeds', COALESCE(userneeds,"")
      )
    ) AS top_articles
  FROM ranked
  WHERE rnk <= 10
  GROUP BY week_number
),

-- All-time weekly totals to find previous low week
all_time_weekly_totals AS (
  SELECT 
    YEARWEEK(date, 3) AS year_week,
    MIN(date) AS week_start,
    MAX(date) AS week_end,
    SUM(visits) AS total_visits
  FROM prod.newsletter
  WHERE post_id IS NOT NULL AND site_id = {siteid}
  GROUP BY YEARWEEK(date, 3)
),

ranked_weeks AS (
  SELECT *,
    DENSE_RANK() OVER (ORDER BY total_visits desc) AS rnk
  FROM all_time_weekly_totals
),

-- Second-lowest total visits week
previous_low_week AS (
  SELECT week_start, week_end
  FROM ranked_weeks
  WHERE rnk = 2
  LIMIT 1
),

-- Label for previous low week
previous_low_week_label AS (
  SELECT 
    CONCAT(DATE_FORMAT(week_start, '%Y-%m-%d'), ' to ', DATE_FORMAT(week_end, '%Y-%m-%d')) AS label
  FROM previous_low_week
),

-- Top 10 from previous low week
previous_low_week_articles AS (
  SELECT 
    n.post_id,
    MAX(p.Title) AS title,
    MAX(p.Date) AS pub_date,
    MAX(p.Link) AS article_url,
    MAX(p.Categories) AS categories,
    MAX(p.Tags) AS tags,
    MAX(p.userneeds) AS userneeds,
    SUM(n.visits) AS total_visits
  FROM prod.newsletter n
  JOIN prod.site_archive_post p ON n.post_id = p.id AND n.site_id = p.siteid
  JOIN previous_low_week w ON n.date BETWEEN w.week_start AND w.week_end
  WHERE n.post_id IS NOT NULL AND n.site_id = {siteid}
      {"AND  p.categories <> 'Nyhedsoverblik'" if siteid == 14 else ""}
{"AND ( \
    NOT ( \
        p.tags IN ( \
            'Billedkunst', 'Børnehaveklassen', 'DSA', 'Engelsk', 'Håndværk og design', \
            'Idræt', 'Kulturfag', 'Lærersenior', 'Lærerstuderende', 'Madkundskab', \
            'Musik', 'Naturfag', 'PLC', 'PPR', 'Praktik', 'SOSU', 'Tysk og fransk', 'UU' \
        ) \
        OR p.categories = 'Seneste nyt' \
    ) \
)" if siteid == 11 else ""}
  GROUP BY n.post_id
  ORDER BY total_visits DESC
  LIMIT 10
),

-- Combine last 3 weeks and low week for share summary
all_relevant_weeks AS (
  SELECT week_start, week_end FROM weeks
  UNION
  SELECT week_start, week_end FROM previous_low_week
),

weekly_shares_with_total AS (
  SELECT 
    CONCAT(DATE_FORMAT(w.week_start, '%Y-%m-%d'), ' to ', DATE_FORMAT(w.week_end, '%Y-%m-%d')) AS week,
    COUNT(*) AS total_visits,
    (
      SELECT count(*) 
      FROM prod.traffic_channels n2
      JOIN all_relevant_weeks w2 ON n2.date BETWEEN w2.week_start AND w2.week_end
      WHERE n2.siteid = {siteid} and n2.realreferrer like 'NL%'
    ) AS grand_total
  FROM prod.traffic_channels n
  JOIN all_relevant_weeks w ON n.date BETWEEN w.week_start AND w.week_end
  WHERE n.siteid = {siteid} and n.realreferrer like 'NL%'
  GROUP BY w.week_start, w.week_end
),

weekly_share_json AS (
  SELECT 
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'week', COALESCE(week,""),
        'total_visits', COALESCE(total_visits,""),
        'share_pct', COALESCE(ROUND(total_visits / grand_total * 100, 1),0)
      )
    ) AS share_summary
  FROM weekly_shares_with_total
)

-- Final output JSON
SELECT JSON_OBJECT(
  'last_week',      (SELECT top_articles FROM json_per_week WHERE week_number = 0),
  'previous_week',  (SELECT top_articles FROM json_per_week WHERE week_number = 1),
  'week_before',    (SELECT top_articles FROM json_per_week WHERE week_number = 2),
  'previous_high_week', (
    SELECT JSON_ARRAYAGG(
      JSON_OBJECT(
        'post_id', COALESCE(post_id,""),
        'pub_date', COALESCE(pub_date,""),
        'total_visits', COALESCE(total_visits,0),
        'categories', COALESCE(categories,""),
        'tags', COALESCE(tags,""),
   
        'userneeds', COALESCE(userneeds,"")
      )
    )
    FROM previous_low_week_articles
  ),
  'weekly_share_summary', JSON_OBJECT(
    'last_three_weeks', (
      SELECT JSON_ARRAYAGG(
        JSON_OBJECT(
          'week', COALESCE(week,""), 
          'total_visits', COALESCE(total_visits,0),
          'share_pct', COALESCE(ROUND(total_visits / grand_total * 100, 1),0)
        )
      )
      FROM (
        SELECT 
          CONCAT(DATE_FORMAT(w.week_start, '%Y-%m-%d'), ' to ', DATE_FORMAT(w.week_end, '%Y-%m-%d')) AS week,
          count(*) AS total_visits,
          (
            SELECT COUNT(*)
            FROM prod.traffic_channels n2
            JOIN all_relevant_weeks w2 ON n2.date BETWEEN w2.week_start AND w2.week_end
            WHERE n2.siteid = {siteid} and n2.realreferrer like 'NL%'
          ) AS grand_total,
          w.week_start
        FROM prod.traffic_channels n
        JOIN weeks w ON n.date BETWEEN w.week_start AND w.week_end
        WHERE n.siteid = {siteid} and n.realreferrer like 'NL%'
        GROUP BY w.week_start, w.week_end
        ORDER BY w.week_start DESC
        LIMIT 3
      ) AS recent_weeks
    ),
    'high', (
      SELECT JSON_ARRAYAGG(
        JSON_OBJECT(
          'week', COALESCE(week,""),
          'total_visits', COALESCE(total_visits,0),
          'share_pct', COALESCE(share_pct,0)
        )
      )
      FROM (
        SELECT 
          CONCAT(DATE_FORMAT(w.week_start, '%Y-%m-%d'), ' to ', DATE_FORMAT(w.week_end, '%Y-%m-%d')) AS week,
          count(*) AS total_visits,
          ROUND(count(*) / (
            SELECT COUNT(*)
            FROM prod.traffic_channels n2
            JOIN all_relevant_weeks w2 ON n2.date BETWEEN w2.week_start AND w2.week_end
            WHERE n2.siteid = {siteid} and n2.realreferrer like 'NL%'
          ) * 100, 1) AS share_pct
        FROM prod.traffic_channels n
        JOIN previous_low_week w ON n.date BETWEEN w.week_start AND w.week_end
        WHERE n.siteid = {siteid} and n.realreferrer like 'NL%'
        GROUP BY w.week_start, w.week_end
      ) AS low_week_data
    )
  )
) AS newsletter_insights;
"""
    scenario_11 = f"""
    WITH weeks AS (
  SELECT 
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 + 7 * n DAY) AS week_end,
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 + 7 * n DAY) AS week_start,
    n AS week_number
  FROM (SELECT 0 AS n UNION ALL SELECT 1 UNION ALL SELECT 2) AS x
),

-- Aggregate weekly visits for last 3 weeks
weekly_visits AS (
  SELECT 
    w.week_number,
    w.week_start,
    w.week_end,
    n.post_id,
    MAX(p.Title) AS title,
    MAX(p.Date) AS pub_date,
    MAX(p.Link) AS article_url,
    MAX(p.Categories) AS categories,
    MAX(p.Tags) AS tags,
    MAX(p.userneeds) AS userneeds,
    SUM(n.visits) AS total_visits
  FROM prod.newsletter n
  JOIN prod.site_archive_post p ON n.post_id = p.id AND n.site_id = p.siteid
  JOIN weeks w ON n.date BETWEEN w.week_start AND w.week_end
  WHERE n.post_id IS NOT NULL AND n.site_id = {siteid}
      {"AND  p.categories <> 'Nyhedsoverblik'" if siteid == 14 else ""}
{"AND ( \
    NOT ( \
        p.tags IN ( \
            'Billedkunst', 'Børnehaveklassen', 'DSA', 'Engelsk', 'Håndværk og design', \
            'Idræt', 'Kulturfag', 'Lærersenior', 'Lærerstuderende', 'Madkundskab', \
            'Musik', 'Naturfag', 'PLC', 'PPR', 'Praktik', 'SOSU', 'Tysk og fransk', 'UU' \
        ) \
        OR p.categories = 'Seneste nyt' \
    ) \
)" if siteid == 11 else ""}
  GROUP BY w.week_number, w.week_start, w.week_end, n.post_id
),

-- Rank articles per week
ranked AS (
  SELECT *,
    ROW_NUMBER() OVER (PARTITION BY week_number ORDER BY total_visits DESC) AS rnk
  FROM weekly_visits
),

-- Get top 10 per recent week
json_per_week AS (
  SELECT 
    week_number,
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'post_id', COALESCE(post_id,""),
        'pub_date', COALESCE(pub_date,""),
        'total_visits', COALESCE(total_visits,0),
        'categories', COALESCE(categories,""),
        'tags', COALESCE(tags,""),
      
        'userneeds', COALESCE(userneeds,"")
      )
    ) AS top_articles
  FROM ranked
  WHERE rnk <= 10
  GROUP BY week_number
),

-- All-time weekly totals to find previous low week
all_time_weekly_totals AS (
  SELECT 
    YEARWEEK(date, 3) AS year_week,
    MIN(date) AS week_start,
    MAX(date) AS week_end,
    SUM(visits) AS total_visits
  FROM prod.newsletter
  WHERE post_id IS NOT NULL AND site_id = {siteid}
  GROUP BY YEARWEEK(date, 3)
),

ranked_weeks AS (
  SELECT *,
    DENSE_RANK() OVER (ORDER BY total_visits ASC) AS rnk
  FROM all_time_weekly_totals
),

-- Second-lowest total visits week
previous_low_week AS (
  SELECT week_start, week_end
  FROM ranked_weeks
  WHERE rnk = 1
  LIMIT 1
),

-- Label for previous low week
previous_low_week_label AS (
  SELECT 
    CONCAT(DATE_FORMAT(week_start, '%Y-%m-%d'), ' to ', DATE_FORMAT(week_end, '%Y-%m-%d')) AS label
  FROM previous_low_week
),

-- Top 10 from previous low week
previous_low_week_articles AS (
  SELECT 
    n.post_id,
    MAX(p.Title) AS title,
    MAX(p.Date) AS pub_date,
    MAX(p.Link) AS article_url,
    MAX(p.Categories) AS categories,
    MAX(p.Tags) AS tags,
    MAX(p.userneeds) AS userneeds,
    SUM(n.visits) AS total_visits
  FROM prod.newsletter n
  JOIN prod.site_archive_post p ON n.post_id = p.id AND n.site_id = p.siteid
  JOIN previous_low_week w ON n.date BETWEEN w.week_start AND w.week_end
  WHERE n.post_id IS NOT NULL AND n.site_id = {siteid}
      {"AND  p.categories <> 'Nyhedsoverblik'" if siteid == 14 else ""}
{"AND ( \
    NOT ( \
        p.tags IN ( \
            'Billedkunst', 'Børnehaveklassen', 'DSA', 'Engelsk', 'Håndværk og design', \
            'Idræt', 'Kulturfag', 'Lærersenior', 'Lærerstuderende', 'Madkundskab', \
            'Musik', 'Naturfag', 'PLC', 'PPR', 'Praktik', 'SOSU', 'Tysk og fransk', 'UU' \
        ) \
        OR p.categories = 'Seneste nyt' \
    ) \
)" if siteid == 11 else ""}
  GROUP BY n.post_id
  ORDER BY total_visits DESC
  LIMIT 10
),

-- Combine last 3 weeks and low week for share summary
all_relevant_weeks AS (
  SELECT week_start, week_end FROM weeks
  UNION
  SELECT week_start, week_end FROM previous_low_week
),

weekly_shares_with_total AS (
  SELECT 
    CONCAT(DATE_FORMAT(w.week_start, '%Y-%m-%d'), ' to ', DATE_FORMAT(w.week_end, '%Y-%m-%d')) AS week,
    COUNT(*) AS total_visits,
    (
      SELECT COUNT(*)
      FROM prod.traffic_channels n2
      JOIN all_relevant_weeks w2 ON n2.date BETWEEN w2.week_start AND w2.week_end
      WHERE n2.siteid = {siteid} and n2.realreferrer like 'NL%'
    ) AS grand_total
  FROM prod.traffic_channels n
  JOIN all_relevant_weeks w ON n.date BETWEEN w.week_start AND w.week_end
  WHERE n.siteid = {siteid} and n.realreferrer like 'NL%'
  GROUP BY w.week_start, w.week_end
),

weekly_share_json AS (
  SELECT 
    JSON_ARRAYAGG(
      JSON_OBJECT(
        'week', COALESCE(week,""),
        'total_visits', COALESCE(total_visits,0),
        'share_pct', COALESCE(ROUND(total_visits / grand_total * 100, 1),0)
      )
    ) AS share_summary
  FROM weekly_shares_with_total
)

-- Final output JSON
SELECT JSON_OBJECT(
  'last_week',      (SELECT top_articles FROM json_per_week WHERE week_number = 0),
  'previous_week',  (SELECT top_articles FROM json_per_week WHERE week_number = 1),
  'week_before',    (SELECT top_articles FROM json_per_week WHERE week_number = 2),
  'previous_low_week', (
    SELECT JSON_ARRAYAGG(
      JSON_OBJECT(
        'post_id', COALESCE(post_id,""),
        'pub_date', COALESCE(pub_date,""),
        'total_visits', COALESCE(total_visits,0),
        'categories', COALESCE(categories,""),
      
        'tags', COALESCE(tags,""),
        'userneeds', COALESCE(userneeds,"")
      )
    )
    FROM previous_low_week_articles
  ),
  'weekly_share_summary', JSON_OBJECT(
  
    -- Last Three Weeks Block (pre-sorted in subquery)
    'last_three_weeks', (
      SELECT JSON_ARRAYAGG(
        JSON_OBJECT(
          'week', COALESCE(week,""),
          'total_visits', COALESCE(total_visits,0),
          'share_pct', COALESCE(ROUND(total_visits / grand_total * 100, 1),0)
        )
      )
      FROM (
        SELECT 
          CONCAT(DATE_FORMAT(w.week_start, '%Y-%m-%d'), ' to ', DATE_FORMAT(w.week_end, '%Y-%m-%d')) AS week,
          count(*) AS total_visits,
          (
            SELECT COUNT(*)
            FROM prod.traffic_channels n2
            JOIN all_relevant_weeks w2 ON n2.date BETWEEN w2.week_start AND w2.week_end
            WHERE n2.siteid = {siteid} and n2.realreferrer like 'NL%'
          ) AS grand_total
        FROM prod.traffic_channels n
        JOIN weeks w ON n.date BETWEEN w.week_start AND w.week_end
        WHERE n.siteid = {siteid} and n.realreferrer like 'NL%'
        GROUP BY w.week_start, w.week_end
        ORDER BY w.week_start DESC
        LIMIT 3
      ) AS ordered_last_three
    ),
    
    -- Low Week Block (grouped safely)
    'low', (
      SELECT JSON_ARRAYAGG(
        JSON_OBJECT(
          'week', CONCAT(DATE_FORMAT(grouped_low.week_start, '%Y-%m-%d'), ' to ', DATE_FORMAT(grouped_low.week_end, '%Y-%m-%d')),
          'total_visits', COALESCE(grouped_low.total_visits,0),
          'share_pct', COALESCE(ROUND(grouped_low.total_visits / grouped_low.grand_total * 100, 1),0)
        )
      )
      FROM (
        SELECT
          w.week_start,
          w.week_end,
          count(*) AS total_visits,
          gt.grand_total
        FROM prod.traffic_channels n
        JOIN previous_low_week w ON n.date BETWEEN w.week_start AND w.week_end
        CROSS JOIN (
          SELECT COUNT(*) AS grand_total
          FROM prod.traffic_channels n2
          JOIN all_relevant_weeks w2 ON n2.date BETWEEN w2.week_start AND w2.week_end
          WHERE n2.siteid = {siteid} and n2.realreferrer like 'NL%'
        ) gt
        WHERE n.siteid = {siteid} and n.realreferrer like 'NL%'
        GROUP BY w.week_start, w.week_end, gt.grand_total
      ) AS grouped_low
    )
  )
) AS newsletter_insights;
"""
    
    scenario_12_20 = f"""
  WITH last_week_articles AS (
    SELECT 
        siteid,
        ID,
        Title,
        userneeds AS Categories,
        COALESCE(tags, "") AS tags,
        COALESCE(Categories, '') AS sektion,
        date AS publish_date,
        link AS url
    FROM prod.site_archive_post
    WHERE  date BETWEEN    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 7) DAY) AND     DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 1) DAY)
      AND siteid = {siteid}     {"AND  categories <> 'Nyhedsoverblik'" if siteid == 14 else ""}
{"AND ( \
    NOT ( \
        tags IN ( \
            'Billedkunst', 'Børnehaveklassen', 'DSA', 'Engelsk', 'Håndværk og design', \
            'Idræt', 'Kulturfag', 'Lærersenior', 'Lærerstuderende', 'Madkundskab', \
            'Musik', 'Naturfag', 'PLC', 'PPR', 'Praktik', 'SOSU', 'Tysk og fransk', 'UU' \
        ) \
        OR categories = 'Seneste nyt' \
    ) \
)" if siteid == 11 else ""}
),
last_week_pageview AS (
    SELECT 
        e.PostID AS postid,
        e.siteid AS siteid,
        COALESCE(SUM(unique_pageviews), 0) AS pageviews 
    FROM last_week_articles ldp 
    JOIN prod.pages e
      ON e.postid = ldp.ID
     AND e.date BETWEEN    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 7) DAY) AND     DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 1) DAY)
     AND e.siteid = {siteid}
    GROUP BY 1,2
),
last_week_clicks AS (
    SELECT 
        e.PostID postid,
        e.siteid AS siteid,
        COALESCE(SUM(hits), 0) AS hits
    FROM last_week_articles ldp 
    JOIN prod.events e
      ON e.postid = ldp.ID
     AND e.date BETWEEN  DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 7) DAY) AND     DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 1) DAY)
     and e.Event_Action = '{event}'
     AND e.siteid = {siteid}
    GROUP BY 1,2
),
last_week_total AS (
    SELECT 
    l.siteid, l.id, l.publish_date, l.Categories, l.tags, l.sektion,
    COALESCE(lp.pageviews, 0) AS pageviews,
    COALESCE(lc.hits, 0) AS hits
    FROM last_week_articles l
    LEFT JOIN last_week_pageview lp ON l.id = lp.postid and l.siteid = lp.siteid
    LEFT JOIN last_week_clicks lc ON l.id = lc.postid and l.siteid = lc.siteid
),
two_weeks_ago_articles AS (
    SELECT 
        siteid,
        ID,
        Title,
        userneeds AS Categories,
        COALESCE(tags, "") AS tags,
        COALESCE(Categories, '') AS sektion,
        date AS publish_date,
        link AS url
    FROM prod.site_archive_post
    WHERE date BETWEEN DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 21) DAY ) AND   DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 8) DAY)
      AND siteid = {siteid}     {"AND  categories <> 'Nyhedsoverblik'" if siteid == 14 else ""}
{"AND ( \
    NOT ( \
        tags IN ( \
            'Billedkunst', 'Børnehaveklassen', 'DSA', 'Engelsk', 'Håndværk og design', \
            'Idræt', 'Kulturfag', 'Lærersenior', 'Lærerstuderende', 'Madkundskab', \
            'Musik', 'Naturfag', 'PLC', 'PPR', 'Praktik', 'SOSU', 'Tysk og fransk', 'UU' \
        ) \
        OR categories = 'Seneste nyt' \
    ) \
)" if siteid == 11 else ""}

), 
two_weeks_ago_pageviews AS (
    SELECT 
        e.PostID AS postid,
        e.siteid AS siteid,
        COALESCE(SUM(unique_pageviews), 0) AS pageviews 
    FROM two_weeks_ago_articles ldp 
    JOIN prod.pages e
      ON e.postid = ldp.ID
     AND e.date  BETWEEN DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 21) DAY ) AND   DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 8) DAY)
     AND e.siteid = {siteid}
    GROUP BY 1,2
),

two_weeks_ago_clicks AS (
    SELECT 
        e.PostID postid,
        e.siteid AS siteid,
        COALESCE(SUM(hits), 0) AS hits
    FROM two_weeks_ago_articles ldp 
    JOIN prod.events e
      ON e.postid = ldp.ID
     AND e.date  BETWEEN DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 21) DAY ) AND   DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 8) DAY)
     and e.Event_Action = '{event}'
     AND e.siteid = {siteid}
    GROUP BY 1,2
),
two_weeks_ago_total as (
    select l.siteid, l.id, l.publish_date, l.Categories, l.tags, l.sektion,
    COALESCE(lp.pageviews, 0) AS pageviews,
    COALESCE(lc.hits, 0) AS hits
    from two_weeks_ago_articles l
    left join two_weeks_ago_pageviews lp on l.id = lp.postid and l.siteid = lp.siteid
    left join two_weeks_ago_clicks lc on l.id = lc.postid and l.siteid = lc.siteid
),

goals AS (
    SELECT 
        MIN(Min_pageviews) AS min_pageviews,
        MIN(Min_CTA) AS min_cta
    FROM prod.goals
    WHERE Site_ID = {siteid}
      AND date BETWEEN    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 7) DAY) AND DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 1) DAY)
)

SELECT JSON_OBJECT(
  'last_week_articles', COALESCE((
    SELECT JSON_ARRAYAGG(
      JSON_OBJECT(
        'post_id', COALESCE(id, ""),
        'pub_date', COALESCE(publish_date, ""),
        'userneeds', COALESCE(Categories, ""),
        'tags', COALESCE(tags, ""),
        'categories', COALESCE(sektion, ""),
        'pageviews', COALESCE(pageviews, 0),
        'next_click_%', COALESCE((hits/pageviews)*100, 0)
      )
    ) FROM last_week_total
  ), JSON_ARRAY(
      JSON_OBJECT(
        'post_id', '',
        'pub_date', '',
        'userneeds', '',
        'tags', '',
        'event_action', '',
        'categories', '',
        'pageviews', 0,
        'next_click_%', 0
      )
  )),

  'two_weeks_ago_articles', COALESCE((
    SELECT JSON_ARRAYAGG(
      JSON_OBJECT(
        'post_id', COALESCE(id, ""),
        'pub_date', COALESCE(publish_date, ""),
        'userneeds', COALESCE(Categories, ""),
        'tags', COALESCE(tags, ""),
        'categories', COALESCE(sektion, ""),
        'pageviews', COALESCE(pageviews, 0),
        'next_click_%', COALESCE((hits/pageviews)*100, 0)
      )
    ) FROM two_weeks_ago_total
  ), JSON_ARRAY(
      JSON_OBJECT(
        'post_id', '',
        'pub_date', '',
        'userneeds', '',
        'tags', '',
        'categories', '',
        'pageviews', 0,
        'next_click_%', 0
      )
  )),

  'goal', COALESCE((
    SELECT JSON_OBJECT(
      'min_pageviews', COALESCE(min_pageviews, 0),
      'min_cta', COALESCE(min_cta, 0)
    )
    FROM goals
  ), JSON_OBJECT(
      'min_pageviews', 0,
      'min_cta', 0
  ))
) AS result_json;"""

    return scenario_7, scenario_8, scenario_9, scenario_10, scenario_11, scenario_12_20
