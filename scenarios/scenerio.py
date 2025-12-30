def format_sql_number(col):
    return f"""CASE 
        WHEN {col} IS NULL THEN 0
        WHEN FLOOR({col}) = {col} 
            THEN CAST({col} AS UNSIGNED) 
        ELSE ROUND({col},1) 
    END"""




def scenerio_query(siteid,event,key,gns):
    special_condition = ""
    if siteid == 14:
        special_condition = "AND categories <> 'Nyhedsoverblik'"
    if siteid == 11:
        special_condition = """and (
      NOT (
    tags IN (
      'Billedkunst', 'Børnehaveklassen', 'DSA', 'Engelsk', 'Håndværk og design',
      'Idræt', 'Kulturfag', 'Lærersenior', 'Lærerstuderende', 'Madkundskab',
      'Musik', 'Naturfag', 'PLC', 'PPR', 'Praktik', 'SOSU', 'Tysk og fransk', 'UU'
    )
    OR categories = 'Seneste nyt'
  )
)"""
    
    sec_1= f"""
           WITH 
date_bounds AS (
  SELECT
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 7) DAY) AS last_monday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 1) DAY) AS last_sunday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 14) DAY) AS prev_monday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 8) DAY) AS prev_sunday
),
last_week_events AS (
  SELECT date, siteid, SUM(hits) AS hits
  FROM prod.events, date_bounds
  WHERE siteid = {siteid} AND event_action = '{event}'
    AND date BETWEEN last_monday AND last_sunday
  GROUP BY date,siteid
),
last_week_pages AS (
  SELECT date,siteid, SUM(unique_pageviews) AS unique_pageviews
  FROM prod.pages, date_bounds
  WHERE siteid = {siteid}
    AND date BETWEEN last_monday AND last_sunday
  GROUP BY date,siteid
),
last_week_ratios AS (
  SELECT
  e.siteid,
  e.hits / p.unique_pageviews AS daily_ratio
  FROM last_week_events e
  JOIN last_week_pages p ON e.date = p.date
 
),
last_Week_final_ratio as(

  select siteid,ROUND((sum(daily_ratio)/count(*) * 100),3) as value_f
            from last_week_ratios
            group by siteid
	

),
previous_week_events AS (
  SELECT date, SUM(hits) AS hits
  FROM prod.events, date_bounds
  WHERE siteid = {siteid} AND event_action = '{event}'
    AND date BETWEEN prev_monday AND prev_sunday
  GROUP BY date
),
previous_week_pages AS (
  SELECT date, SUM(unique_pageviews) AS unique_pageviews
  FROM prod.pages, date_bounds
  WHERE siteid = {siteid}
    AND date BETWEEN prev_monday AND prev_sunday
  GROUP BY date
),
previous_week_ratios AS (
  SELECT
       e.hits / p.unique_pageviews AS daily_ratio
  FROM previous_week_events e
  JOIN previous_week_pages p ON e.date = p.date
),
previous_week_ratios_final as (
select ROUND((sum(daily_ratio)/count(*) * 100),3) as value_f
            from previous_week_ratios
),


last_week_total AS (
  SELECT SUM(value_f) AS total_ratio FROM last_Week_final_ratio
),
previous_week_total AS (
  SELECT SUM(value_f) AS total_ratio FROM previous_week_ratios_final
),
cta_goal_total AS (
  SELECT avg(cta_per_day) AS cta_total
  FROM prod.goals, date_bounds
  WHERE site_id = {siteid} AND date BETWEEN last_monday AND last_sunday

),
besog_data AS (
  SELECT
    SUM(visits) AS total_visits,
    (SELECT SUM(visits_per_day)
     FROM prod.goals, date_bounds
     WHERE site_id = {siteid} AND date BETWEEN last_monday AND last_sunday
    ) AS goal_visits,
    (SELECT SUM(visits)
     FROM prod.daily_totals, date_bounds
     WHERE siteid = {siteid} AND date BETWEEN prev_monday AND prev_sunday
    ) AS previous_visits
  FROM prod.daily_totals, date_bounds
  WHERE siteid = {siteid} AND date BETWEEN last_monday AND last_sunday
),
sidevisninger_data AS (
  SELECT
    SUM(unique_pageviews) AS total_pageviews,
    (SELECT SUM(pageviews_per_day)
     FROM prod.goals, date_bounds
     WHERE site_id = {siteid} AND date BETWEEN last_monday AND last_sunday
    ) AS goal_pageviews,
    (SELECT SUM(unique_pageviews)
     FROM prod.daily_totals, date_bounds
     WHERE siteid = {siteid} AND date BETWEEN prev_monday AND prev_sunday
    ) AS previous_pageviews
  FROM prod.daily_totals, date_bounds
  WHERE siteid = {siteid} AND date BETWEEN last_monday AND last_sunday
),
table1_json AS (
   SELECT
    CONCAT(
      '{{',
       '"data": {{',
            '"sidevisninger": ', JSON_OBJECT(
                'total', {format_sql_number('s.total_pageviews')},
                'afvigelse_ift_maal', CONCAT(
                    CASE WHEN s.goal_pageviews = 0 THEN ''
                        WHEN ((s.total_pageviews - s.goal_pageviews) / s.goal_pageviews) >= 0 THEN '+'
                        ELSE ''
                    END,
                    COALESCE(ROUND(
                        CASE
                        WHEN s.goal_pageviews = 0 THEN 0
                        ELSE ((s.total_pageviews - s.goal_pageviews) / s.goal_pageviews) * 100
                        END, 1
                    ),0), '%'
                ),
                'udvikling_ift_forrige_uge', CONCAT(
                    CASE WHEN s.previous_pageviews = 0 THEN ''
                        WHEN ((s.total_pageviews - s.previous_pageviews) / s.previous_pageviews) >= 0 THEN '+'
                        ELSE ''
                    END,
                    COALESCE(ROUND(
                        CASE
                        WHEN s.previous_pageviews = 0 THEN 0
                        ELSE ((s.total_pageviews - s.previous_pageviews) / s.previous_pageviews) * 100
                        END, 1
                    ),0), '%'
                )
            ), ',',
            '"besog": ', JSON_OBJECT(
                'total', {format_sql_number('b.total_visits')},
                'afvigelse_ift_maal', CONCAT(
                    CASE WHEN b.goal_visits = 0 THEN ''
                        WHEN ((b.total_visits - b.goal_visits) / b.goal_visits) >= 0 THEN '+'
                        ELSE ''
                    END,
                    COALESCE(ROUND(
                        CASE
                        WHEN b.goal_visits = 0 THEN 0
                        ELSE ((b.total_visits - b.goal_visits) / b.goal_visits) * 100
                        END, 1
                    ),0), '%'
                ),
                'udvikling_ift_forrige_uge', CONCAT(
                    CASE WHEN b.previous_visits = 0 THEN ''
                        WHEN ((b.total_visits - b.previous_visits) / b.previous_visits) >= 0 THEN '+'
                        ELSE ''
                    END,
                    COALESCE(ROUND(
                        CASE
                        WHEN b.previous_visits = 0 THEN 0
                        ELSE ((b.total_visits - b.previous_visits) / b.previous_visits) * 100
                        END, 1
                    ),0), '%'
                )
            ), ',',
            '"{key}": ', JSON_OBJECT(
                'total', {format_sql_number('l.total_ratio')},
                'afvigelse_ift_maal', CONCAT(
                    CASE WHEN g.cta_total = 0 THEN '0.0'
                        WHEN l.total_ratio / g.cta_total - 1 >= 0 THEN '+'
                        ELSE '-'
                    END,
                    COALESCE(ROUND(ABS(l.total_ratio / g.cta_total - 1) * 100, 1),0), '%'
                ),
                'udvikling_ift_forrige_uge', CONCAT(
                    CASE WHEN p.total_ratio = 0 THEN '0.0'
                        WHEN l.total_ratio / p.total_ratio - 1 >= 0 THEN '+'
                        ELSE '-'
                    END,
                    COALESCE(ROUND(ABS(l.total_ratio / p.total_ratio - 1) * 100, 1),0), '%'
                )
            ),
            '}}',
        '}}'
        ) AS json_output
  FROM last_week_total l, previous_week_total p, cta_goal_total g, besog_data b, sidevisninger_data s
),

-- ---------- table2 CTEs ----------
known_referrers AS (
  SELECT DISTINCT realreferrer
  FROM pre_stage.ref_value
  WHERE siteid = {siteid}
),
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
week_bounds AS (
  SELECT
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 7) DAY) AS last_monday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 1) DAY) AS last_sunday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 14) DAY) AS prev_monday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 8) DAY) AS prev_sunday
),
total_counts AS (
  SELECT SUM(weekly_count) AS total_count
  FROM (
    SELECT mapped_referrer, COUNT(*) AS weekly_count
    FROM traffic_mapped, week_bounds
    WHERE date BETWEEN last_monday AND last_sunday
    GROUP BY mapped_referrer
  ) AS sub
),
last_week_counts AS (
  SELECT mapped_referrer AS realreferrer, COUNT(*) AS weekly_count
  FROM traffic_mapped, week_bounds
  WHERE date BETWEEN last_monday AND last_sunday
  GROUP BY mapped_referrer
),
previous_week_counts AS (
  SELECT mapped_referrer AS realreferrer, COUNT(*) AS prev_weekly_count
  FROM traffic_mapped, week_bounds
  WHERE date BETWEEN prev_monday AND prev_sunday
  GROUP BY mapped_referrer
),
combined_traffic AS (
  SELECT 
    l.realreferrer,
    tc.total_count,
    COALESCE(l.weekly_count, 0) AS weekly_count,
    COALESCE(p.prev_weekly_count, 0) AS prev_weekly_count
  FROM last_week_counts l
  LEFT JOIN previous_week_counts p ON l.realreferrer = p.realreferrer
  CROSS JOIN total_counts tc
),
table2_json AS (
     SELECT JSON_OBJECT(
            'data', JSON_ARRAYAGG(
             JSON_OBJECT(
                'kanal', COALESCE(realreferrer,''),
                'besog_uge', {format_sql_number('weekly_count')},
                'andel_af_total', CONCAT(
                    {format_sql_number("""CASE 
                        WHEN total_count = 0 THEN 0
                        ELSE (weekly_count / total_count) * 100
                    END""")},
                    '%'
                ),
                'udvikling_ift_forrige_uge', CONCAT(
                    CASE
                        WHEN prev_weekly_count = 0 THEN '0.0'
                        WHEN (weekly_count - prev_weekly_count) / prev_weekly_count >= 0 THEN '+'
                        ELSE '-'
                    END,
                    {format_sql_number("""CASE 
                        WHEN prev_weekly_count = 0 THEN 0
                        ELSE ABS((weekly_count - prev_weekly_count) / prev_weekly_count) * 100
                    END""")},
                    '%'
                )
            )
        )
    ) AS json_output
    FROM combined_traffic
),
date_bounds_345 AS (
  SELECT
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) AS week_start,
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AS week_end
),

last_week_articles AS (
  SELECT sa.id, sa.date, sa.siteid, sa.userneeds
  FROM prod.site_archive_post sa
   JOIN date_bounds_345  db
    ON sa.date BETWEEN db.week_start AND db.week_end
  WHERE sa.siteid = {siteid} {special_condition}
),
article_daily_views AS (
  SELECT 
    p.siteid,
    p.postid,
    l.date,
    sum(p.unique_pageviews) AS pageviews
  FROM last_week_articles l
  JOIN prod.pages p
    ON p.postid = l.id AND p.siteid = l.siteid
  left JOIN date_bounds_345 db
    ON p.date BETWEEN db.week_start AND db.week_end
  WHERE p.siteid = {siteid}
  group by 1,2,3
),

article_daily_cta AS (
  SELECT 
    e.siteid,
    e.postid,
    l.date,
    sum(e.hits) AS hits
  FROM last_week_articles l

  left JOIN  prod.events e 
  ON e.postid = l.id AND e.siteid = l.siteid
  left JOIN date_bounds_345 db
    ON e.date BETWEEN db.week_start AND db.week_end
  WHERE e.siteid = {siteid}
    AND e.event_action = '{event}'
    group by 1,2,3
), 
  missing_post_in_pages as(
        select * from article_daily_views
        union
        select ldp.siteid, id as PostID ,ldp.date as date, 0  as pageviews
        
        from last_week_articles ldp
        left join article_daily_views lp on lp.postid = ldp.id
        where lp.siteid is null
) ,

hits_pages as (
select e.siteid, e.postid, e.date, e.hits, p.pageviews
from  article_daily_cta e
left join missing_post_in_pages p  on p.postid =e.postid and e.siteid =p.siteid
union
select p.siteid, p.postid,p.date, e.hits ,p.pageviews
from  article_daily_cta e
right join missing_post_in_pages p on p.postid = e.postid and e.siteid =p.siteid
),
hits_pages_goal as(
select 
l.postid,l.siteid,l.date,
 case 
                    when 
                        (
                            coalesce( (coalesce(l.hits,0) / coalesce(l.pageviews,0)) * 100 , 0)<coalesce(g.Min_CTA,0) 
                        and 
                        coalesce(l.pageviews,0)<coalesce(g.Min_pageviews,0) )
                        then 1 else 0 end as below_goal,
case 
		when 
            (coalesce((coalesce(l.hits,0)/coalesce(l.pageviews,1)*100),0)>=coalesce(g.Min_CTA,0) 
              or coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) 
            )
          and 
                !(coalesce((coalesce(l.hits,0)/coalesce(l.pageviews,1)*100),0)>=coalesce(g.Min_CTA,0) 
            and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) 
          ) 
          then 1 else 0 end as one_of_goal,
 case when ((coalesce(coalesce(l.hits,0)/coalesce(l.pageviews,0)*100,0))>=coalesce(g.Min_CTA,0) and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )
        then 1 else 0 end as above_goal  
from hits_pages l
  left join  prod.goals g on g.date=l.date and g.site_id = l.siteid 
  where l.siteid ={siteid}

),
table_3 as (
SELECT  
  COUNT(DISTINCT lwa.id) AS article_count,
  ROUND(AVG(pg.pageviews), 2) AS avg_pageviews,
  ROUND(SUM(dc.hits)/sum(pg.pageviews) * 100, 2) AS 'avg_next_clicks'

FROM last_week_articles lwa
LEFT JOIN article_daily_views pg ON lwa.id = pg.postid
LEFT JOIN article_daily_cta dc ON lwa.id = dc.postid
),
table_4 as (
select 
(sum(g.below_goal)/count(l.id))* 100 as below_goal_percentage,
(sum(g.one_of_goal)/count(l.id))* 100 as one_of_goal_percentage,
(sum(g.above_goal)/count(l.id))* 100 as above_goal_percentage
from last_week_articles l 
join hits_pages_goal g  on l.id = g.postid  and l.siteid = g.siteid
),
combined AS (
  SELECT 
    lwa.userneeds AS brugerbehov,
    COUNT(DISTINCT lwa.id) AS article_count,
    COALESCE(SUM(cta.hits), 0) AS total_hits,
    COALESCE(SUM(view.pageviews), 0) AS total_pageviews
  FROM last_week_articles lwa
  LEFT JOIN article_daily_cta cta ON cta.postid = lwa.id
  LEFT JOIN article_daily_views view ON view.postid = lwa.id
  GROUP BY lwa.userneeds
),

total_articles AS (
  SELECT COUNT(*) AS total FROM last_week_articles
),

table_5 AS (
  SELECT
    c.brugerbehov,
    ROUND((c.article_count * 100.0) / t.total, 2) AS andel,
    ROUND(COALESCE(c.total_hits, 0) * 1.0 / NULLIF(c.total_pageviews, 0) * 100, 2) AS gns_cta,
    ROUND(COALESCE(c.total_pageviews, 0) * 1.0 / NULLIF(c.article_count, 0), 2) AS gns_sidevisninger
  FROM combined c, total_articles t
  ORDER BY andel DESC
),
json_f AS (
        SELECT 
        JSON_OBJECT(
            'table3',JSON_OBJECT(
                 '{gns}',  {format_sql_number('t3.avg_next_clicks')},
                    'antal_artikler', {format_sql_number('t3.article_count')},
                    'gns_sidevisninger', {format_sql_number('t3.avg_pageviews')}
                ),
                'table4', JSON_OBJECT(
                 'over_et_maal',  {format_sql_number('t4.one_of_goal_percentage')},
                    'over_begge_maal', {format_sql_number('t4.above_goal_percentage')},
                    'under_begge_maal', {format_sql_number('t4.below_goal_percentage')}
                ),
                'table5', COALESCE((
                SELECT JSON_ARRAYAGG(
                    JSON_OBJECT(
                    'brugerbehov', t5.brugerbehov,
                    'andel', {format_sql_number('t5.andel')},
                    '{gns}', {format_sql_number('t5.gns_cta')},
                    'gns_sidevisninger', {format_sql_number('t5.gns_sidevisninger')}
                    )
                )
                FROM table_5 t5
                ), JSON_ARRAY(JSON_OBJECT(
                'brugerbehov', '',
                'andel', '',
                '{gns}', '',
                'gns_sidevisninger', ''
                ) ))
            )
            AS result_json
        FROM table_3 t3, table_4 t4)
SELECT JSON_OBJECT(
  'table1', JSON_OBJECT(
    'data', JSON_EXTRACT(t1.json_output, '$.data')),
	'table2', JSON_OBJECT(
    'data', JSON_EXTRACT(t2.json_output, '$.data')),
	'table3', JSON_EXTRACT(t3.result_json, '$."table3"'),
    'table4', JSON_EXTRACT(t3.result_json, '$."table4"'),
    'table5', JSON_EXTRACT(t3.result_json, '$."table5"')
)
 AS full_json_output
FROM table1_json t1, table2_json t2, json_f t3;"""
    


    sec_2 = f"""
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
      AND siteid = {siteid} {special_condition}
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
      AND siteid = {siteid} {special_condition}

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
        'pageviews', COALESCE({format_sql_number('pageviews')}, 0),
        '{key}', COALESCE(({format_sql_number('hits')}/{format_sql_number('pageviews')})*100, 0)
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
        'pageviews', COALESCE({format_sql_number('pageviews')}, 0),
        '{key}', COALESCE(({format_sql_number('hits')}/{format_sql_number('pageviews')})*100, 0)
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
) AS result_json;
            """
    sec_3 =f"""WITH 
date_bounds AS (
  SELECT
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 7) DAY) AS last_monday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 1) DAY) AS last_sunday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 14) DAY) AS prev_monday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 8) DAY) AS prev_sunday
),
last_week_events AS (
  SELECT date, siteid, SUM(hits) AS hits
  FROM prod.events, date_bounds
  WHERE siteid = {siteid} AND event_action = '{event}'
    AND date BETWEEN last_monday AND last_sunday
  GROUP BY date,siteid
),
last_week_pages AS (
  SELECT date,siteid, SUM(unique_pageviews) AS unique_pageviews
  FROM prod.pages, date_bounds
  WHERE siteid = {siteid}
    AND date BETWEEN last_monday AND last_sunday
  GROUP BY date,siteid
),
last_week_ratios AS (
  SELECT
  e.siteid,
  e.hits / p.unique_pageviews AS daily_ratio
  FROM last_week_events e
  JOIN last_week_pages p ON e.date = p.date
 
),
last_Week_final_ratio as(

  select siteid,ROUND((sum(daily_ratio)/count(*) * 100),3) as value_f
            from last_week_ratios
            group by siteid
	

),
previous_week_events AS (
  SELECT date, SUM(hits) AS hits
  FROM prod.events, date_bounds
  WHERE siteid = {siteid} AND event_action = '{event}'
    AND date BETWEEN prev_monday AND prev_sunday
  GROUP BY date
),
previous_week_pages AS (
  SELECT date, SUM(unique_pageviews) AS unique_pageviews
  FROM prod.pages, date_bounds
  WHERE siteid = {siteid}
    AND date BETWEEN prev_monday AND prev_sunday
  GROUP BY date
),
previous_week_ratios AS (
  SELECT
       e.hits / p.unique_pageviews AS daily_ratio
  FROM previous_week_events e
  JOIN previous_week_pages p ON e.date = p.date
),
previous_week_ratios_final as (
select ROUND((sum(daily_ratio)/count(*) * 100),3) as value_f
            from previous_week_ratios
),


last_week_total AS (
  SELECT SUM(value_f) AS total_ratio FROM last_Week_final_ratio
),
previous_week_total AS (
  SELECT SUM(value_f) AS total_ratio FROM previous_week_ratios_final
),
cta_goal_total AS (
  SELECT avg(cta_per_day) AS cta_total
  FROM prod.goals, date_bounds
  WHERE site_id = {siteid} AND date BETWEEN last_monday AND last_sunday

),
besog_data AS (
  SELECT
    SUM(visits) AS total_visits,
    (SELECT SUM(visits_per_day)
     FROM prod.goals, date_bounds
     WHERE site_id = {siteid} AND date BETWEEN last_monday AND last_sunday
    ) AS goal_visits,
    (SELECT SUM(visits)
     FROM prod.daily_totals, date_bounds
     WHERE siteid = {siteid} AND date BETWEEN prev_monday AND prev_sunday
    ) AS previous_visits
  FROM prod.daily_totals, date_bounds
  WHERE siteid = {siteid} AND date BETWEEN last_monday AND last_sunday
),
sidevisninger_data AS (
  SELECT
    SUM(unique_pageviews) AS total_pageviews,
    (SELECT SUM(pageviews_per_day)
     FROM prod.goals, date_bounds
     WHERE site_id = {siteid} AND date BETWEEN last_monday AND last_sunday
    ) AS goal_pageviews,
    (SELECT SUM(unique_pageviews)
     FROM prod.daily_totals, date_bounds
     WHERE siteid = {siteid} AND date BETWEEN prev_monday AND prev_sunday
    ) AS previous_pageviews
  FROM prod.daily_totals, date_bounds
  WHERE siteid = {siteid} AND date BETWEEN last_monday AND last_sunday
),
table1_json AS (
   SELECT
    CONCAT(
      '{{',
       '"data": {{',
            '"sidevisninger": ', JSON_OBJECT(
                'total', {format_sql_number('s.total_pageviews')},
                'afvigelse_ift_maal', CONCAT(
                    CASE WHEN s.goal_pageviews = 0 THEN ''
                        WHEN ((s.total_pageviews - s.goal_pageviews) / s.goal_pageviews) >= 0 THEN '+'
                        ELSE ''
                    END,
                    COALESCE(ROUND(
                        CASE
                        WHEN s.goal_pageviews = 0 THEN 0
                        ELSE ((s.total_pageviews - s.goal_pageviews) / s.goal_pageviews) * 100
                        END, 1
                    ),0), '%'
                ),
                'udvikling_ift_forrige_uge', CONCAT(
                    CASE WHEN s.previous_pageviews = 0 THEN ''
                        WHEN ((s.total_pageviews - s.previous_pageviews) / s.previous_pageviews) >= 0 THEN '+'
                        ELSE ''
                    END,
                    COALESCE(ROUND(
                        CASE
                        WHEN s.previous_pageviews = 0 THEN 0
                        ELSE ((s.total_pageviews - s.previous_pageviews) / s.previous_pageviews) * 100
                        END, 1
                    ),0), '%'
                )
            ), ',',
            '"besog": ', JSON_OBJECT(
                'total', {format_sql_number('b.total_visits')},
                'afvigelse_ift_maal', CONCAT(
                    CASE WHEN b.goal_visits = 0 THEN ''
                        WHEN ((b.total_visits - b.goal_visits) / b.goal_visits) >= 0 THEN '+'
                        ELSE ''
                    END,
                    COALESCE(ROUND(
                        CASE
                        WHEN b.goal_visits = 0 THEN 0
                        ELSE ((b.total_visits - b.goal_visits) / b.goal_visits) * 100
                        END, 1
                    ),0), '%'
                ),
                'udvikling_ift_forrige_uge', CONCAT(
                    CASE WHEN b.previous_visits = 0 THEN ''
                        WHEN ((b.total_visits - b.previous_visits) / b.previous_visits) >= 0 THEN '+'
                        ELSE ''
                    END,
                    COALESCE(ROUND(
                        CASE
                        WHEN b.previous_visits = 0 THEN 0
                        ELSE ((b.total_visits - b.previous_visits) / b.previous_visits) * 100
                        END, 1
                    ),0), '%'
                )
            ), ',',
            '"{key}": ', JSON_OBJECT(
                'total', {format_sql_number('l.total_ratio')},
                'afvigelse_ift_maal', CONCAT(
                    CASE WHEN g.cta_total = 0 THEN '0.0'
                        WHEN l.total_ratio / g.cta_total - 1 >= 0 THEN '+'
                        ELSE '-'
                    END,
                    COALESCE(ROUND(ABS(l.total_ratio / g.cta_total - 1) * 100, 1),0), '%'
                ),
                'udvikling_ift_forrige_uge', CONCAT(
                    CASE WHEN p.total_ratio = 0 THEN '0.0'
                        WHEN l.total_ratio / p.total_ratio - 1 >= 0 THEN '+'
                        ELSE '-'
                    END,
                    COALESCE(ROUND(ABS(l.total_ratio / p.total_ratio - 1) * 100, 1),0), '%'
                )
            ),
            '}}',
        '}}'
        ) AS json_output
  FROM last_week_total l, previous_week_total p, cta_goal_total g, besog_data b, sidevisninger_data s
),

-- ---------- table2 CTEs ----------
known_referrers AS (
  SELECT DISTINCT realreferrer
  FROM pre_stage.ref_value
  WHERE siteid = {siteid}
),
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
week_bounds AS (
  SELECT
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 7) DAY) AS last_monday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 1) DAY) AS last_sunday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 14) DAY) AS prev_monday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 8) DAY) AS prev_sunday
),
total_counts AS (
  SELECT SUM(weekly_count) AS total_count
  FROM (
    SELECT mapped_referrer, COUNT(*) AS weekly_count
    FROM traffic_mapped, week_bounds
    WHERE date BETWEEN last_monday AND last_sunday
    GROUP BY mapped_referrer
  ) AS sub
),
last_week_counts AS (
  SELECT mapped_referrer AS realreferrer, COUNT(*) AS weekly_count
  FROM traffic_mapped, week_bounds
  WHERE date BETWEEN last_monday AND last_sunday
  GROUP BY mapped_referrer
),
previous_week_counts AS (
  SELECT mapped_referrer AS realreferrer, COUNT(*) AS prev_weekly_count
  FROM traffic_mapped, week_bounds
  WHERE date BETWEEN prev_monday AND prev_sunday
  GROUP BY mapped_referrer
),
combined_traffic AS (
  SELECT 
    l.realreferrer,
    tc.total_count,
    COALESCE(l.weekly_count, 0) AS weekly_count,
    COALESCE(p.prev_weekly_count, 0) AS prev_weekly_count
  FROM last_week_counts l
  LEFT JOIN previous_week_counts p ON l.realreferrer = p.realreferrer
  CROSS JOIN total_counts tc
),
table2_json AS (
        SELECT JSON_OBJECT(
            'data', JSON_ARRAYAGG(
             JSON_OBJECT(
                'kanal', COALESCE(realreferrer,''),
                'besog_uge', {format_sql_number('weekly_count')},
                'andel_af_total', CONCAT(
                    {format_sql_number("""CASE 
                        WHEN total_count = 0 THEN 0
                        ELSE (weekly_count / total_count) * 100
                    END""")},
                    '%'
                ),
                'udvikling_ift_forrige_uge', CONCAT(
                    CASE
                        WHEN prev_weekly_count = 0 THEN '0.0'
                        WHEN (weekly_count - prev_weekly_count) / prev_weekly_count >= 0 THEN '+'
                        ELSE '-'
                    END,
                    {format_sql_number("""CASE 
                        WHEN prev_weekly_count = 0 THEN 0
                        ELSE ABS((weekly_count - prev_weekly_count) / prev_weekly_count) * 100
                    END""")},
                    '%'
                )
            )
        )
    ) AS json_output
    FROM combined_traffic
),
date_bounds_345 AS (
  SELECT
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) AS week_start,
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AS week_end
),

last_week_articles AS (
  SELECT sa.id, sa.date, sa.siteid, sa.userneeds
  FROM prod.site_archive_post sa
   JOIN date_bounds_345  db
    ON sa.date BETWEEN db.week_start AND db.week_end
  WHERE sa.siteid = {siteid}  {special_condition}
),
article_daily_views AS (
  SELECT 
    p.siteid,
    p.postid,
    l.date,
    sum(p.unique_pageviews) AS pageviews
  FROM last_week_articles l
  JOIN prod.pages p
    ON p.postid = l.id AND p.siteid = l.siteid
  left JOIN date_bounds_345 db
    ON p.date BETWEEN db.week_start AND db.week_end
  WHERE p.siteid = {siteid}
  group by 1,2,3
),

article_daily_cta AS (
  SELECT 
    e.siteid,
    e.postid,
    l.date,
    sum(e.hits) AS hits
  FROM last_week_articles l

  left JOIN  prod.events e 
  ON e.postid = l.id AND e.siteid = l.siteid
  left JOIN date_bounds_345 db
    ON e.date BETWEEN db.week_start AND db.week_end
  WHERE e.siteid = {siteid}
    AND e.event_action = '{event}'
    group by 1,2,3
), 
  missing_post_in_pages as(
        select * from article_daily_views
        union
        select ldp.siteid, id as PostID ,ldp.date as date, 0  as pageviews
        
        from last_week_articles ldp
        left join article_daily_views lp on lp.postid = ldp.id
        where lp.siteid is null
) ,

hits_pages as (
select e.siteid, e.postid, e.date, e.hits, p.pageviews
from  article_daily_cta e
left join missing_post_in_pages p  on p.postid =e.postid and e.siteid =p.siteid
union
select p.siteid, p.postid,p.date, e.hits ,p.pageviews
from  article_daily_cta e
right join missing_post_in_pages p on p.postid = e.postid and e.siteid =p.siteid
),
hits_pages_goal as(
select 
l.postid,l.siteid,l.date,
 case 
                    when 
                        (
                            coalesce( (coalesce(l.hits,0) / coalesce(l.pageviews,0)) * 100 , 0)<coalesce(g.Min_CTA,0) 
                        and 
                        coalesce(l.pageviews,0)<coalesce(g.Min_pageviews,0) )
                        then 1 else 0 end as below_goal,
case 
		when 
            (coalesce((coalesce(l.hits,0)/coalesce(l.pageviews,1)*100),0)>=coalesce(g.Min_CTA,0) 
              or coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) 
            )
          and 
                !(coalesce((coalesce(l.hits,0)/coalesce(l.pageviews,1)*100),0)>=coalesce(g.Min_CTA,0) 
            and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) 
          ) 
          then 1 else 0 end as one_of_goal,
 case when ((coalesce(coalesce(l.hits,0)/coalesce(l.pageviews,0)*100,0))>=coalesce(g.Min_CTA,0) and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )
        then 1 else 0 end as above_goal  
from hits_pages l
  left join  prod.goals g on g.date=l.date and g.site_id = l.siteid 
  where l.siteid ={siteid}

),
table_3 as (
SELECT  
  COUNT(DISTINCT lwa.id) AS article_count,
  ROUND(AVG(pg.pageviews), 2) AS avg_pageviews,
  ROUND(SUM(dc.hits)/sum(pg.pageviews) * 100, 2) AS avg_next_clicks
FROM last_week_articles lwa
LEFT JOIN article_daily_views pg ON lwa.id = pg.postid
LEFT JOIN article_daily_cta dc ON lwa.id = dc.postid
),
table_4 as (
select 
(sum(g.below_goal)/count(l.id))* 100 as below_goal_percentage,
(sum(g.one_of_goal)/count(l.id))* 100 as one_of_goal_percentage,
(sum(g.above_goal)/count(l.id))* 100 as above_goal_percentage
from last_week_articles l 
join hits_pages_goal g  on l.id = g.postid  and l.siteid = g.siteid
),
combined AS (
  SELECT 
    lwa.userneeds AS brugerbehov,
    COUNT(DISTINCT lwa.id) AS article_count,
    COALESCE(SUM(cta.hits), 0) AS total_hits,
    COALESCE(SUM(view.pageviews), 0) AS total_pageviews
  FROM last_week_articles lwa
  LEFT JOIN article_daily_cta cta ON cta.postid = lwa.id
  LEFT JOIN article_daily_views view ON view.postid = lwa.id
  GROUP BY lwa.userneeds
),

total_articles AS (
  SELECT COUNT(*) AS total FROM last_week_articles
),

table_5 AS (
  SELECT
    c.brugerbehov,
    ROUND((c.article_count * 100.0) / t.total, 2) AS andel,
    ROUND(COALESCE(c.total_hits, 0) * 1.0 / NULLIF(c.total_pageviews, 0) * 100, 2) AS gns_cta,
    ROUND(COALESCE(c.total_pageviews, 0) * 1.0 / NULLIF(c.article_count, 0), 2) AS gns_sidevisninger
  FROM combined c, total_articles t
  ORDER BY andel DESC
),
json_f AS (
        SELECT 
        JSON_OBJECT(
           'table3', JSON_OBJECT(

                 '{gns}',  {format_sql_number('t3.avg_next_clicks')},
                    'antal_artikler', {format_sql_number('t3.article_count')},
                    'gns_sidevisninger', {format_sql_number('t3.avg_pageviews')}
                ),
                'table4', JSON_OBJECT(
                 'over_et_maal',  {format_sql_number('t4.one_of_goal_percentage')},
                    'over_begge_maal', {format_sql_number('t4.above_goal_percentage')},
                    'under_begge_maal', {format_sql_number('t4.below_goal_percentage')}
                ),
                'table5', COALESCE((
                SELECT JSON_ARRAYAGG(
                    JSON_OBJECT(
                    'brugerbehov', t5.brugerbehov,
                    'andel', {format_sql_number('t5.andel')},
                    '{gns}', {format_sql_number('t5.gns_cta')},
                    'gns_sidevisninger', {format_sql_number('t5.gns_sidevisninger')}
                    )
                )
                FROM table_5 t5
                ), JSON_ARRAY(JSON_OBJECT(
                'brugerbehov', '',
                'andel', '',
                '{gns}', '',
                'gns_sidevisninger', ''
                )))
            )
             AS result_json
        FROM table_3 t3, table_4 t4)

-- ---------- Final Combined Output ----------
SELECT JSON_OBJECT(
  'site', {siteid},
  "uge", "uge35",
   "prev_uge", "uge34",
  'data',JSON_OBJECT(
  'table1', JSON_OBJECT(
    'data', JSON_EXTRACT(t1.json_output, '$.data')),
	'table2', JSON_OBJECT(
    'data', JSON_EXTRACT(t2.json_output, '$.data')),
	'table3', JSON_EXTRACT(t3.result_json, '$."table3"'),
    'table4', JSON_EXTRACT(t3.result_json, '$."table4"'),
    'table5', JSON_EXTRACT(t3.result_json, '$."table5"')
)
  )
 AS full_json_output
FROM table1_json t1, table2_json t2, json_f t3;

"""
    return sec_1, sec_2, sec_3