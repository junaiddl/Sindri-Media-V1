


    






CREATE PROCEDURE `tactical_clicks_months_dbt_4`()
BEGIN
WITH 
last_30_days_article_published AS (
    SELECT siteid as siteid,id   
    FROM prod.site_archive_post
    WHERE 
    date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
    AND siteid = 4
    
),

last_30_days_article_published_Agg AS (
    SELECT siteid as siteid,count(*) as agg 
    FROM  last_30_days_article_published
    WHERE siteid = 4
    group by 1
),

agg_last_30_days AS
(
    SELECT  e.siteid as siteid,sum(hits)/agg as value from prod.events e
    JOIN  last_30_days_article_published a on a.id=e.postid
    JOIN last_30_days_article_published_Agg agg on agg.siteid=a.siteid 
    WHERE date  between DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY) 
    AND e.siteid = 4
    AND e.Event_Action = 'Newsletter'
    GROUP by 1,agg
),

last_30_days_before_article_published AS
(
    SELECT siteId as siteId,id
    FROM prod.site_archive_post
    WHERE date  between DATE_SUB(NOW(), INTERVAL 60 DAY) and DATE_SUB(NOW(), INTERVAL 30 DAY)
    AND  siteid = 4
    
),


last_30_days_before_article_published_Agg AS (
    SELECT siteid as siteid,count(*) as agg 
    FROM last_30_days_before_article_published
    WHERE siteid = 4
    GROUP by 1
),

agg_last_30_days_before AS (
    SELECT  e.siteid as siteid,sum(hits)/agg as value from prod.events e
    JOIN  last_30_days_before_article_published a on a.id=e.postid
    JOIN last_30_days_before_article_published_Agg agg on agg.siteid=a.siteid 
    WHERE date  between DATE_SUB(NOW(), INTERVAL 60 DAY) and DATE_SUB(NOW(), INTERVAL 30 DAY) 
    AND e.siteid = 4
    AND e.Event_Action = 'Newsletter'
    GROUP by 1,agg
)

SELECT 
    JSON_OBJECT(
        'site', al.siteid,
        'data', JSON_OBJECT(
            'label', 'Gns. sign-ups pr artikel',
            'hint', 'Gennemsnitligt antal nyhedsbrev sign-ups på artikler publiceret seneste 30 dage ift. forrige 30 dage',
            'value', COALESCE(al.value, 0),
            'change', COALESCE(round(((al.value-alb.value)/al.value)*100,2),0),
            'progressCurrent', '',
            'progressTotal', ''
        )
    ) AS json_data
FROM agg_last_30_days al
LEFT JOIN agg_last_30_days_before alb ON al.siteid = alb.siteid
WHERE al.siteid = 4;

END

