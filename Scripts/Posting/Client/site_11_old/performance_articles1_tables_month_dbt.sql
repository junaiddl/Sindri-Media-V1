


            
        
        
        
        
        
        
        
        
        


        CREATE  PROCEDURE `performance_articles1_tables_month_dbt_11`()
        BEGIN

        WITH last_30days_post AS (
            SELECT 
                siteid as siteid,
                ID,
                Title,
        userneeds
 AS Categories, 
        tags
 AS tags,
        Categories
 AS sektion,
                date as Date,
                link as url,
               
                DATE(Modified) as  updated
                
            FROM prod.site_archive_post
            where date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) 
            
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

            
            and siteid = 11 
        ),
        last_30days_hits as(
            select 
            coalesce(e.siteid,11) as siteid,
                coalesce(e.PostID,ldp.Id) as postid,
                ldp.Categories,
                ldp.Title,
                ldp.date as publish_date,
                coalesce(sum(hits),0) as hits,
                ldp.url,
                ldp.updated,
                ldp.tags,
                ldp.sektion
                
            from  last_30days_post ldp
        left join prod.events e
        on e.postid=ldp.id and e.siteid = ldp.siteid
        and e.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) and e.siteid = 11
        and e.Event_Action= 'Next Click'
        group by 1,2,3,4,5,7,8,9,10
            
        ),
        last_30days_pageview as(
        select 
                coalesce(e.siteid,11) as siteid,
                coalesce(e.PostID,ldp.Id) as postid,
                ldp.Categories,
                ldp.Title,
                ldp.date as publish_date,
                coalesce(sum(unique_pageviews),0) as pageviews,
                ldp.url,
                ldp.updated,
                ldp.tags,
                ldp.sektion
                    
            from last_30days_post ldp
        join prod.pages e
        on e.postid=ldp.id and e.siteid = ldp.siteid
        and e.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) and e.siteid = 11
        group by 1,2,3,4,5,7,8,9,10
            
        ),
        last_30days_hits_pages as(
        select l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews,l.Categories,l.Title, l.url, l.updated,l.tags,l.sektion 
            
        from last_30days_hits l
        left join last_30days_pageview p on l.postid=p.postId and l.siteid = p.siteid
        where  l.siteid = 11
        union 
        select p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews,p.Categories,p.Title , p.url, p.updated, p.tags,p.sektion 
            
        from last_30days_hits l
        right join last_30days_pageview p on l.postid=p.postId and l.siteid = p.siteid
        where p.siteid = 11
        ),

        last_30days_hits_pages_goal as (
            select  
            l.siteid AS siteid,
                l.postid,
                l.publish_date,
                l.hits,
                l.pageviews,
                SUM(l.hits) AS s_hits,
                SUM(l.pageviews) AS s_pageviews,
                l.Categories,
                l.Title,
                l.url,
                l.updated,
                g.min_pageviews,
                g.Min_CTA,
                l.tags,
                l.sektion,
                case 
                    when 
                        (
                            coalesce( coalesce((coalesce(l.hits,0) / coalesce(l.pageviews,0)),0) * 100 , 0)>=coalesce(g.Min_CTA,0)
                            or 
                            coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) 
                        )
                        and 
                        !(
                            (coalesce( coalesce((coalesce(l.hits,0) / coalesce(l.pageviews,0)),0) * 100 , 0))>=coalesce(g.Min_CTA,0) 
                        and 
                        coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) ) 
                    then 1 else 0 end as gt_goal  
                
                    from last_30days_hits_pages l
            join  prod.goals g on g.date=l.publish_date  and g.site_id = l.siteid
            where  g.site_id = 11
            group by 1,2,3,4,5,8,9,10,11,12,13,14,15,16
                
        )
        select 
        CONCAT('{"site":', siteid,',"data":{','"columns":[{"field": "id", "label": "ID"}, {"field": "article", "label": "ARTIKEL"}, {"field": "category", "label": "BRUGERBEHOV"}, {"field": "tags", "label": "Tags", "hidden": true}, {"field": "sektion", "label": "Sektion", "hidden": true}, {"field": "date", "label": "PUB DATO"}, {"field": "brugerbehov", "label": "SIDEVISNINGER"}, {"field": "clicks", "label": "Next Click (%) "}],','"rows":',JSON_ARRAYAGG(
                JSON_OBJECT(
                    'id', postid,
                    'article', Title,
                    'category', coalesce(Categories, ''),
                        'sektion',coalesce(sektion , ''),
                        'tags', coalesce(tags, ''),
                        
                        'date', publish_date,
                        'updated', updated,
                        'url', url,
                    'brugerbehov', coalesce(pageviews,0),
                'clicks',ROUND(coalesce(lg.s_hits, 0) / coalesce(lg.s_pageviews, 0), 3)*100
                ))
            ,'}}') AS json_data

        from last_30days_hits_pages_goal lg where gt_goal=1 and siteid = 11;

        END


