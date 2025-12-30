


       
     
        
        
        
        
        
        
        
        
        

        CREATE DEFINER=`Site`@`%` PROCEDURE `performance_articles1_tables_ytd_dbt_16`()
        BEGIN

        WITH last_ytd_post AS (
            SELECT siteid as siteid,ID,Title,
        userneeds
 AS Categories, 
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
            Date as Date, link as url, DATE(Modified) as  updated
        
        
        
            FROM prod.site_archive_post 
            where date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) 
            
            and siteid = 16 
            ),

        last_ytd_hits as (
            SELECT 
                COALESCE(e.siteid, 16) AS siteid,
                COALESCE(e.PostID, ldp.Id) AS postid,
                ldp.Categories,
                MAX(ldp.Title) AS Title,
                ldp.date AS publish_date,
                COALESCE(SUM(hits), 0) AS hits,
                ldp.url,
                ldp.updated,
                ldp.tags,
                ldp.sektion
                
            FROM   
                last_ytd_post ldp
                LEFT JOIN prod.events e ON e.postid = ldp.id AND e.siteid = ldp.siteid AND e.date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY) AND e.siteid = 16 AND e.Event_Action =  'Next Click'
            GROUP BY 1, 2, 3, 5, 7, 8, 9,10
            
        ),

        last_ytd_pageview as (
            SELECT 
                COALESCE(e.siteid, 16) AS siteid,
                COALESCE(e.PostID, ldp.Id) AS postid,
                ldp.Categories,
                MAX(ldp.Title) AS Title,
                ldp.date AS publish_date,
                COALESCE(SUM(unique_pageviews), 0) AS pageviews,
                ldp.url,
                ldp.updated,
                ldp.tags,
                ldp.sektion
                
            FROM 
                last_ytd_post ldp
                LEFT JOIN prod.pages e ON e.postid = ldp.id AND e.siteid = ldp.siteid AND e.date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY) AND e.siteid = 16
            GROUP BY 1, 2, 3, 5, 7, 8, 9,10
            
        ),


        last_ytd_hits_pages as(
        select l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews,l.Categories,l.Title,l.url, l.updated, l.tags,l.sektion  
        
        from last_ytd_hits l
        left join last_ytd_pageview p on l.postid=p.postId  and l.siteid=p.siteid
        where l.siteid = 16
        union 
        select p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews,p.Categories,p.Title ,p.url, p.updated, p.tags,p.sektion 
        
        from last_ytd_hits l
        right join last_ytd_pageview p on l.postid=p.postId and p.siteid=l.siteid
        where p.siteid = 16
        ),

        last_ytd_hits_pages_goal as (
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
            case when ((coalesce(coalesce(l.hits,0)/coalesce(l.pageviews,0),0)*100)>=coalesce(g.Min_CTA,0) or coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )
        and !((coalesce(coalesce(l.hits,0)/coalesce(l.pageviews,0),0)*100)>=coalesce(g.Min_CTA,0) and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) ) then 1 else 0 end as gt_goal
        
                from last_ytd_hits_pages l
            join  prod.goals g on g.date=l.publish_date and g.site_id = l.siteid
            where  g.site_id = 16
            GROUP BY   1,2,3,4,5,8,9,10,11,12,13, 14,15
            
        )

        select 
        CONCAT('{"site":', siteid,',"data":{','"columns":[{"field": "id", "label": "ID"}, {"field": "article", "label": "ARTIKEL"}, {"field": "category", "label": "BRUGERBEHOV"}, {"field": "tags", "label": "FOKUSOMRÅDER", "hidden": true}, {"field": "date", "label": "DATO"}, {"field": "brugerbehov", "label": "SIDEVISNINGER"}, {"field": "clicks", "label": "NEXT CLICK (%)"}],','"rows":',JSON_ARRAYAGG(
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


        from last_ytd_hits_pages_goal lg where gt_goal  =1 and siteid = 16;



        END

