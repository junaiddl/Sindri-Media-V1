


    








CREATE PROCEDURE `tactical_articles_table_ytd_dbt_16`()
BEGIN
    with last_30_days_article_published as(
        SELECT siteid as siteid,id,title as article,
        userneeds
 AS category, 
        CASE
  WHEN tags LIKE '%Løn%' THEN REPLACE(tags, 'Løn', 'Løn og konkrete fordele')
  WHEN tags LIKE '%Arbejdsmiljø%' THEN REPLACE(tags, 'Arbejdsmiljø', 'Arbejdsmiljø')
  WHEN tags LIKE '%Arbejdsliv%' THEN REPLACE(tags, 'Arbejdsliv', 'Det moderne arbejdsliv')
  WHEN tags LIKE '%Udannelse%' THEN REPLACE(tags, 'Udannelse', 'Uddannelse')
  WHEN tags LIKE '%Grøn_omstilling%' THEN REPLACE(tags, 'Grøn_omstilling', 'Grøn omstilling')
  WHEN tags LIKE '%Formanden%' THEN REPLACE(tags, 'Formanden', 'Formanden har ordet')
  ELSE 'Others'
END
 AS tags,
        Categories
 AS sektion,
        link as url, DATE(Modified) as  updated,
        date as date  FROM prod.site_archive_post
        where 
        date  between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) 
        and siteid = 16 
        
    ),
    uniquepages as (
        select  e.siteid as siteid,e.postid,sum(unique_pageviews) as uniq_val from prod.pages e
        join  last_30_days_article_published a on a.id=e.postid
        where  e.siteid = 16
        group by 1,2
    ),
    cta_per_article as (
        select  e.siteid as siteid,e.postid,sum(hits) as val_hits from prod.events e
        left join  last_30_days_article_published a on a.id=e.postid
        where Event_Action = 'Next Click' and  e.siteid = 16
        group by 1,2
    )

    select 
    CONCAT('{"site":',  l.siteid,',"data":{','"columns": [{"field": "id", "label": "ID"}, {"field": "article", "label": "ARTIKEL"}, {"field": "category", "label": "BRUGERBEHOV"}, {"field": "tags", "label": "FOKUSOMRÅDER", "hidden": true}, {"field": "date", "label": "DATO"}, {"field": "brugerbehov", "label": "SIDEVISNINGER"}, {"field": "clicks", "label": "NEXT CLICK (%)"}],','"rows":',JSON_ARRAYAGG(
            JSON_OBJECT(
                'id', id,
                'article', article,
                'category', l.category,
                'tags',l.tags,
                'sektion',l.sektion,
                'date', date,
                'updated', updated,
                'url', url,
                'brugerbehov',coalesce(uniq_val,0),
                'clicks',ROUND(coalesce((coalesce(val_hits,0)/coalesce(uniq_val,0)) * 100,0),1)  
            ))
        ,'}}'
    ) AS json_data
    from last_30_days_article_published l
    left join uniquepages up
    on l.siteid = up.siteid and l.id = up.postid
    left join cta_per_article ca
    on l.siteid = ca.siteid and l.id = ca.postid 
    where  l.siteid = 16;
END

