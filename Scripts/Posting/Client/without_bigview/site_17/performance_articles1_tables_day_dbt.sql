


       
    
    
    
    
    
    
    
    
    
    


    CREATE PROCEDURE `performance_articles1_tables_day_dbt_17`()
    BEGIN
    WITH last_7days_post AS (
        SELECT siteid as siteid,ID,Title, 
        userneeds
 AS Categories, 
        tags
 AS tags,
        Categories
 AS sektion,
        Date as Date,link as url, DATE(Modified) as  updated
        
        FROM prod.site_archive_post
        where date  between DATE_SUB(NOW(), INTERVAL 8 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY)   
        
        and siteid = 17 
    ),
    last_7days_hits as(
      select 
        coalesce(e.siteid,17) as siteid,
            coalesce(e.PostID,ldp.Id) as postid,
            ldp.Categories,
            ldp.Title,
            ldp.date as publish_date,
            ldp.url, 
            ldp.updated,
            ldp.Tags,
            ldp.sektion,
            coalesce(sum(hits),0) as hits
              
      from  last_7days_post ldp
      left join prod.events e
      on e.postid=ldp.id   and e.siteid = ldp.siteid
      AND e.date between DATE_SUB(NOW(), INTERVAL 8 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY)  and e.siteid = 17
      and e.Event_Action= 'Next Click'
      group by 1,2,3,4,5,6,7,8,9
          
    ) ,
    last_7days_pageview as(
      select 
        coalesce(e.siteid,17) as siteid,
            coalesce(e.PostID,ldp.Id) as postid,
            ldp.Categories,
            ldp.Title,
            ldp.date as publish_date,
            ldp.url,
            ldp.updated,
            ldp.Tags,
            ldp.sektion,
            COALESCE(sum(unique_pageviews),0) as pageviews 
              
      from  last_7days_post ldp
      join prod.pages e
      on e.postid=ldp.id   and e.siteid = ldp.siteid
      and e.date between DATE_SUB(NOW(), INTERVAL 8 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) and e.siteid = 17
      group by 1,2,3,4,5,6,7,8,9
          
    ),

    last_7days_hits_pages as(
    select l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews,l.Categories,l.Title, l.url, l.updated, l.Tags,l.sektion
      
    from last_7days_hits l
    left join last_7days_pageview p on l.postid=p.postId   and l.siteid = p.siteid
    where l.siteid = 17
    union 
    select p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews,p.Categories,p.Title ,p.url, p.updated, p.Tags,p.sektion 
      
    from last_7days_hits l
    right join last_7days_pageview p on l.postid=p.postId   and l.siteid = p.siteid
    where p.siteid = 17
    ),

    last_7days_hits_pages_goal AS (
        SELECT 
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
            l.Tags,
            g.min_pageviews,
            g.Min_CTA,
            l.sektion,
    case 
          when 
            (coalesce((coalesce(l.hits,0)/coalesce(l.pageviews,1)*100),0)>=coalesce(g.Min_CTA,0) 
              or coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) 
            )
          and 
                !(coalesce((coalesce(l.hits,0)/coalesce(l.pageviews,1)*100),0)>=coalesce(g.Min_CTA,0) 
            and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) 
          ) 
          then 1 else 0 end as gt_goal     
          
      
        FROM last_7days_hits_pages l
        JOIN prod.goals g ON g.date = l.publish_date AND g.site_id = l.siteid
        WHERE g.site_id = 17
        GROUP BY 
            l.siteid,
            l.postid,
            l.publish_date,
            l.hits,
            l.pageviews,
            l.Categories,
            l.Title,
            l.url,
            l.updated,
            l.Tags,
            g.min_pageviews,
            g.Min_CTA,
            l.sektion
              
    )

    select 
    CONCAT('{"site":', siteid,',"data":{','"columns":[{"field": "id", "label": "ID"}, {"field": "article", "label": "ARTIKEL"}, {"field": "category", "label": "BRUGERBEHOV"}, {"field": "sektion", "label": "KATEGORI", "hidden": true}, {"field": "tags", "label": "Tags", "hidden": true}, {"field": "date", "label": "DATO"}, {"field": "brugerbehov", "label": "SIDEVISNINGER"}, {"field": "clicks", "label": "NEXT CLICK (%)"}],','"rows":',JSON_ARRAYAGG(
              JSON_OBJECT(
                  'id', postid,
                  'article', Title,
                  'category', Categories,
                  'sektion',sektion,
                    
                  'date', publish_date,
                  'updated', updated,
                  'url', url,
                  'tags', Tags,
                  'brugerbehov', coalesce(pageviews,0),
            'clicks',coalesce(ROUND(coalesce(lg.s_hits, 0) / coalesce(lg.s_pageviews, 0), 3)*100 ,0) 
              ))
          ,'}}') AS json_data

    from last_7days_hits_pages_goal lg where gt_goal=1 and siteid = 17;

    END


