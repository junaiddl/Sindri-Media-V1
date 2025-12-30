


    








CREATE PROCEDURE `tactical_articles_table_ytd_dbt_11`()
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
        and siteid = 11 
        
        AND (
  NOT 
  (
      tags REGEXP "(^|[ ,])(billedkunst|børnehaveklassen|dsa|engelsk|håndværk og design|idræt|kulturfag|lærersenior|lærerstuderende|madkundskab|musik|naturfag|plc|ppr|praktik|sosu|tyskfransk|uu)([ ,]|$)"
  )
  OR 
  (
      tags REGEXP "(^|[ ,])(billedkunst|børnehaveklassen|dsa|engelsk|håndværk og design|idræt|kulturfag|lærersenior|lærerstuderende|madkundskab|musik|naturfag|plc|ppr|praktik|sosu|tyskfransk|uu)([ ,]|$)"
      AND tags REGEXP "(^|[ ,])(dansk|it|matematik|specialpædagogik|Skolepolitik|DLF|Skoleledelse|Psykisk arbejdsmiljø|Forskning)([,]|$)"
  )
)

        
    ),
    uniquepages as (
        select  e.siteid as siteid,e.postid,sum(unique_pageviews) as uniq_val from prod.pages e
        join  last_30_days_article_published a on a.id=e.postid
        where  e.siteid = 11
        and e.date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) 
        group by 1,2
    ),
    cta_per_article as (
        select  e.siteid as siteid,e.postid,sum(hits) as val_hits from prod.events e
        left join  last_30_days_article_published a on a.id=e.postid
        where Event_Action = 'Next Click' and  e.siteid = 11
        and e.date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) 
        group by 1,2
    )

    select 
    CONCAT('{"site":',  l.siteid,',"data":{','"columns": [{"field": "id", "label": "ID"}, {"field": "article", "label": "ARTIKEL"}, {"field": "category", "label": "BRUGERBEHOV"}, {"field": "tags", "label": "Tags", "hidden": true}, {"field": "sektion", "label": "Sektion", "hidden": true}, {"field": "date", "label": "PUB DATO"}, {"field": "brugerbehov", "label": "SIDEVISNINGER"}, {"field": "clicks", "label": "Next Click (%) "}],','"rows":',JSON_ARRAYAGG(
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
    where  l.siteid = 11;
END

