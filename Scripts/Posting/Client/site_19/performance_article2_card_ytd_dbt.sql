


            
        
        
        
        

        CREATE PROCEDURE `performance_articles2_card_ytd_dbt_19`()
        BEGIN
        WITH last_ytd_post AS (
            SELECT siteid as siteid,ID, Date as Date
            FROM prod.site_archive_post
            where date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) 
        
            AND tags <> 'Pages'
            
            and siteid = 19
            ),
        last_ytd_before_post AS (
            SELECT siteid as siteid,ID, date as Date
            FROM prod.site_archive_post
            where date  between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year)
            
            AND tags <> 'Pages'
            
            and siteid = 19
        ),
        last_ytd_hits as(
            select 
                coalesce(e.siteid,19) as siteid,
                coalesce(e.PostID,ldp.ID) as postid,
                ldp.date as publish_date,
                coalesce(sum(hits),0) as hits 
            from  last_ytd_post ldp
            left join prod.events e
            on e.postid=ldp.id and ldp.siteid=e.siteid
            and e.date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) 
            and e.siteid = 19
            and e.Event_Action = "Next Click"
            group by 1,2,3
        ),
        last_ytd_before_hits as(
            select 
                coalesce(e.siteid,19) as siteid,
                coalesce(e.PostID,ldp.ID) as postid,
                ldp.date as publish_date,
                coalesce(sum(hits),0) as hits 
            from  last_ytd_before_post ldp
            join prod.events e
            on e.postid=ldp.id
            and e.date between  DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year)
            and e.siteid = 19
            and e.Event_Action = "Next Click"
            group by 1,2,3
        ),
        last_ytd_pageview as(
            select 
                coalesce(e.siteid,19) as siteid,
                coalesce(e.PostID,ldp.ID) as postid,
                ldp.date as publish_date,
                coalesce(sum(unique_pageviews),0) as pageviews 
            from  last_ytd_post ldp
            join prod.pages e
            on e.postid=ldp.id and ldp.siteid=e.siteid
            and e.date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) 
            and e.siteid = 19
            group by 1,2,3
        ),
        missing_post_in_pages as(
        select * from last_ytd_pageview
        union
        select ldp.siteid, id as postid ,ldp.date as publish_date,0  as pageviews from last_ytd_post ldp
        left join last_ytd_pageview lp on lp.postid = ldp.id
        where lp.siteid is null
        ),
        last_ytd_pageview_before as(
        select e.siteid as siteid,e.PostID,ldp.date as publish_date,sum(unique_pageviews) as pageviews from  last_ytd_before_post ldp
        join prod.pages e
        on e.postid=ldp.id
        where e.date between  DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year) 
        and e.siteid = 19
        group by 1,2,3
        ),
        before_missing_post_in_pages as(
        select * from last_ytd_pageview_before
        union
        select ldp.siteid, id as postid ,ldp.date as publish_date,0  as pageviews from last_ytd_before_post ldp
        left join last_ytd_pageview_before lp on lp.postid = ldp.id
        where lp.siteid is null
        ),
        last_ytd_hits_pages as(
        select l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews from last_ytd_hits l
        left join missing_post_in_pages p on l.postid=p.postId
        where  l.siteid =19
        union  
        select p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews from last_ytd_hits l
        right join missing_post_in_pages p on l.postid=p.postId
        where  p.siteid =19
        ),
        last_ytd_hits_pages_before as(
        select l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews from last_ytd_before_hits l
        left join before_missing_post_in_pages p on l.postid=p.postId
        where  l.siteid = 19
        union  
        select p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews from last_ytd_before_hits l
        right join before_missing_post_in_pages p on l.postid=p.postId
        where  l.siteid = 19
        ),
        last_ytd_hits_pages_goal as (
        select l.*,g.min_pageviews,g.Min_CTA,
        case when ((coalesce(coalesce(l.hits,0)/coalesce(l.pageviews,0)*100,0))>=coalesce(g.Min_CTA,0) and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )then 1 else 0 end as gt_goal

        from last_ytd_hits_pages l
        join  prod.goals g on g.date=l.publish_date and l.siteid=g.site_id
        where g.site_id = 19
        ),
        last_ytd_hits_pages_goal_before as (
        select l.*,g.min_pageviews,g.Min_CTA,
        case when ((coalesce(coalesce(l.hits,0)/coalesce(l.pageviews,0)*100,0))>=coalesce(g.Min_CTA,0) and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )then 1 else 0 end as gt_goal
        from last_ytd_hits_pages_before l
        join  prod.goals g on g.date=l.publish_date and l.siteid=g.site_id
        where g.site_id = 19
        ) ,
        last_goal_achived_public_article as(
        select siteid,count(*) as total_publish_article,sum(gt_goal) as  value
        from last_ytd_hits_pages_goal
        where  siteid = 19
        group by 1
        ),
        last_goal_achived_public_article_before as(
        select siteid,count(*) as total_publish_article,sum(gt_goal) as  value
        from last_ytd_hits_pages_goal_before
        where  siteid = 19
        group by 1
        )
        select 
        JSON_OBJECT(
                "data", JSON_OBJECT(
                    "label", "Artikler over på begge mål",
                    "hint",  "Artikler publiceret år til dato, der når begge mål ",
                    "value", lg.value,
                    "change", coalesce(round(((lg.value-pg.value)/pg.value)*100,2),0)
                ),
                'site', lg.siteid
            ) AS json_output
        from last_goal_achived_public_article lg
        left join last_goal_achived_public_article_before pg on lg.siteid=pg.siteid;

        END


