


      

        
        
        
        
        
        
        
        



        CREATE PROCEDURE `performance_articles2_tables_day_dbt_10`()
        BEGIN
        WITH last_7days_post AS (
            SELECT siteid as siteid,ID,Title, userneeds as Categories, coalesce(tags, "") as tags,COALESCE(Categories, '') AS sektion, Date as Date,link as url, DATE(Modified) as  updated
            
            FROM prod.site_archive_post 
            where date between DATE_SUB(NOW(), INTERVAL 8 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) 
        
            and siteid = 10 
        ),
        last_7days_hits as(
            select 
                coalesce(e.siteid,10) as siteid,
                coalesce(e.PostID,ldp.Id) as postid,
                ldp.Categories,
                ldp.Title,
                ldp.date as publish_date, 
                ldp.url,
                ldp.updated,
                coalesce(sum(hits),0) as hits,
                ldp.tags,
                ldp.sektion
                
            from  last_7days_post ldp
            left join prod.events e
            on e.postid=ldp.id   and e.siteid = ldp.siteid
            and e.date between DATE_SUB(NOW(), INTERVAL 8 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY)  and e.siteid = 10
            and e.Event_Action= 'Newsletter'
            group by 1,2,3,4,5,6,7,9,10
            
        ) ,
        last_7days_pageview as(
            select 
                coalesce(e.siteid,10) as siteid,
                coalesce(e.PostID,ldp.Id) as postid,
                ldp.Categories,
                ldp.Title,
                ldp.date as publish_date,
                ldp.url,
                ldp.updated,
                coalesce(sum(unique_pageviews),0) as pageviews,
                ldp.tags,
                ldp.sektion
                
            from  last_7days_post ldp
            join prod.pages e
            on e.postid=ldp.id   and e.siteid = ldp.siteid
            and e.date between DATE_SUB(NOW(), INTERVAL 8 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY)  and e.siteid = 10
            group by 1,2,3,4,5,6,7,9,10
            
        ),

        last_7days_hits_pages as(
        select l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews,l.Categories,l.Title ,  l.url, l.updated, l.tags,l.sektion 
        

        from last_7days_hits l
        left join last_7days_pageview p on l.postid=p.postId and p.siteid = l.siteid
        where  l.siteid = 10
        union 
        select p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews,p.Categories,p.Title, p.url, p.updated, p.tags,p.sektion  
        
        from last_7days_hits l
        right join last_7days_pageview p on l.postid=p.postId and p.siteid = l.siteid
        where  p.siteid = 10

        ),
        last_7days_hits_pages_goal as (
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
                g.min_pageviews,
                g.Min_CTA,
                l.tags,
                l.sektion,
       case when (coalesce(l.hits,0)>=coalesce(g.Min_CTA,0) and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )
 then 1 else 0 end as gt_goal 
            
        
        from last_7days_hits_pages l
        join  prod.goals g on g.date=l.publish_date and g.site_id = l.siteid
        where  l.siteid = 10
        GROUP BY   1,2,3,4,5,8,9,10,11,12,13,14,15
        

        )
        select 
        CONCAT('{"site":', siteid,',"data":{','"columns":[{"field": "id","label": "ID"},{"field": "article","label": "ARTIKEL"},{"field": "sektion","label": "KATEGORI"},{"field": "tags","label": "Tags","hidden": true},{"field": "date","label": "PUB DATO"},{"field": "brugerbehov","label": "SIDEVISNINGER"},{"field": "clicks","label": "Next Click (%)"}],','"rows":',JSON_ARRAYAGG(
                JSON_OBJECT(
                    'id', postid,
                    'article', Title,
                    'category', Categories,
                        'sektion',sektion,
                        'tags', tags,
                        
                        'date', publish_date,
                        'updated', updated,
                        'url', url,
                    'brugerbehov', coalesce(pageviews,0),
                     'clicks',coalesce(hits,0)  
                ))
            ,'}}') AS json_data

        from last_7days_hits_pages_goal lg where gt_goal =1 and siteid=10;




        END


