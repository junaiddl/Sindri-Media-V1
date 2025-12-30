


    






CREATE PROCEDURE `tactical_clicks_ytd_dbt_18`()
BEGIN
WITH 
last_30_days_article_published AS (
    SELECT siteid as siteid,id   
    FROM prod.site_archive_post
    WHERE 
    date  BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
    AND siteid = 18
    
),

last_30_days_article_published_Agg AS (
    SELECT siteid as siteid,count(*) as agg 
    FROM  last_30_days_article_published
    WHERE siteid = 18
    group by 1
),

agg_last_30_days AS
(
    SELECT  e.siteid as siteid,sum(hits)/agg as value from prod.events e
    JOIN  last_30_days_article_published a on a.id=e.postid
    JOIN last_30_days_article_published_Agg agg on agg.siteid=a.siteid 
    WHERE date  BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
    AND e.siteid = 18
    AND e.Event_Action = 'Sales'
    GROUP by 1,agg
),

last_30_days_before_article_published AS
(
    SELECT siteId as siteId,id
    FROM prod.site_archive_post
    WHERE date  between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year) 
    AND  siteid = 18
    
),


last_30_days_before_article_published_Agg AS (
    SELECT siteid as siteid,count(*) as agg 
    FROM last_30_days_before_article_published
    WHERE siteid = 18
    GROUP by 1
),

agg_last_30_days_before AS (
    SELECT  e.siteid as siteid,sum(hits)/agg as value from prod.events e
    JOIN  last_30_days_before_article_published a on a.id=e.postid
    JOIN last_30_days_before_article_published_Agg agg on agg.siteid=a.siteid 
    WHERE date  between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year) AND DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year) 
    AND e.siteid = 18
    AND e.Event_Action = 'Sales'
    GROUP by 1,agg
)

SELECT 
    JSON_OBJECT(
        'site', COALESCE(al.siteid, 18),
        'data', JSON_OBJECT(
            'label', 'Gns. klik på køb',
            'hint', 'Gennemsnitligt klik på køb på artikler publiceret år til dato ift. sidste år',
            'value', COALESCE(al.value, 0),
            'change', 
                CASE 
                    WHEN alb.value IS NULL OR alb.value = 0 THEN 0 
                    ELSE COALESCE(ROUND(((al.value - alb.value) / alb.value) * 100, 2), 0)
                END,
            'progressCurrent', '',
            'progressTotal', ''
        )
    ) AS json_data
FROM 
    (SELECT 18 AS siteid) AS dummy
    LEFT JOIN agg_last_30_days al ON al.siteid = dummy.siteid
    LEFT JOIN agg_last_30_days_before alb ON al.siteid = alb.siteid;


END

