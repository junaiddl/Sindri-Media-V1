


    







CREATE PROCEDURE `tactical_articles_table_ytd_dbt_10`()
BEGIN
    with last_30_days_article_published as(
        SELECT siteid as siteid,id,title as article,
        userneeds
 AS category, 
        tags
 AS tags,
        Categories
 AS sektion,
        link as url, DATE(Modified) as  updated,
        date as date  FROM prod.site_archive_post
        where 
        date  between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) 
        and siteid = 10 
        
    ),
    uniquepages as (
        select  e.siteid as siteid,e.postid,sum(unique_pageviews) as uniq_val from prod.pages e
        join  last_30_days_article_published a on a.id=e.postid
        where  e.siteid = 10
        group by 1,2
    ),
    cta_per_article as (
        select  e.siteid as siteid,e.postid,sum(hits) as val_hits from prod.events e
        left join  last_30_days_article_published a on a.id=e.postid
        where Event_Action = 'Newsletter' and  e.siteid = 10
        group by 1,2
    )

    select 
    CONCAT('{"site":',  l.siteid,',"data":{','"columns": [{"field": "id","label": "ID"},{"field": "article","label": "ARTIKEL"},{"field": "sektion","label": "KATEGORI"},{"field": "tags","label": "Tags","hidden": true},{"field": "date","label": "PUB DATO"},{"field": "brugerbehov","label": "SIDEVISNINGER"},{"field": "clicks","label": "Next Click (%)"}],','"rows":',JSON_ARRAYAGG(
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
                'clicks',coalesce(val_hits,0)
            ))
        ,'}}'
    ) AS json_data
    from last_30_days_article_published l
    left join uniquepages up
    on l.siteid = up.siteid and l.id = up.postid
    left join cta_per_article ca
    on l.siteid = ca.siteid and l.id = ca.postid 
    where  l.siteid = 10;
END

