 {% macro newsletter_performance_articles2_card_ytd_dbt() %}       
        {% set site = var('site') %}
        {% set event = var('event_action') %}
        {% set status = var('condition')['status'] %}
        {% set query = var('condition')['query'] %}

        CREATE PROCEDURE `performance_articles2_card_ytd_dbt_{{site}}`()
        BEGIN
       
            WITH last_ytd_post AS (
                SELECT siteid as siteid,ID, date as Date
                FROM prod.site_archive_post
                where date  between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)  
            {% if status %}
                {{query}}
                {% endif %}
                and siteid = {{site}}
            ),
            last_ytd_before_days_post AS (
                SELECT siteid as siteid,ID, date as Date
                FROM prod.site_archive_post
                where date  between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year)
                {% if status %}
                {{query}}
                {% endif %}
                and siteid = {{site}}
            ),
            last_ytd_hits as(
            select 
                    coalesce(e.siteid,{{site}}) as siteid,
                    coalesce(e.PostID,ldp.ID) as postid,
                    ldp.date as publish_date,
                    coalesce(sum(hits),0) as hits
                from last_ytd_post ldp
            join prod.events e
            on e.postid=ldp.id and e.siteid = ldp.siteid
            and e.date  between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)  and e.siteid = {{site}}
            and e.Event_Action='{{event}}'
            group by 1,2,3
            ),
            last_ytd_before_hits as(
            select 
                    coalesce(e.siteid,{{site}}) as siteid,
                    coalesce(e.PostID,ldp.ID) as postid,
                    ldp.date as publish_date,
                    coalesce(sum(hits),0) as hits 
                from  last_ytd_before_days_post ldp
            join prod.events e
            on e.postid=ldp.id and e.siteid = ldp.siteid
            and e.date  between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year)  
            and e.Event_Action='{{event}}'
            and e.siteid = {{site}}
            group by 1,2,3
            ),
            last_ytd_pageview as(
            select 
                    COALESCE(e.siteid,{{site}}) as siteid,
                    COALESCE(e.PostID,ldp.ID) as postid,
                    ldp.date as publish_date,
                    coalesce(sum(unique_pageviews),0) as pageviews 
                from last_ytd_post ldp
            join prod.pages e
            on e.postid=ldp.id and e.siteid = ldp.siteid
            where e.date  between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) and e.siteid = {{site}}
            group by 1,2,3
            ),
            last_ytd_pageview_before as(
            select 
                    coalesce(e.siteid,{{site}}) as siteid,
                    coalesce(e.PostID,ldp.ID) as postid,
                    ldp.date as publish_date,
                    coalesce(sum(unique_pageviews),0) as pageviews 
                from last_ytd_before_days_post ldp
            join prod.pages e
            on e.postid=ldp.id and e.siteid = ldp.siteid
            where e.date  between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year)  
            and e.siteid = {{site}}
            group by 1,2,3
            ),
            last_ytd_hits_pages as(
            select l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews from last_ytd_hits l
            left join last_ytd_pageview p on l.postid=p.postId and l.siteid = p.siteid
            where l.siteid = {{site}}
            union 
            select p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews from last_ytd_hits l
            right join last_ytd_pageview p on l.postid=p.postId and l.siteid = p.siteid
            where  p.siteid = {{site}}
            ),
            last_ytd_hits_pages_before as(
            select l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews from last_ytd_before_hits l
            left join last_ytd_pageview_before p on l.postid=p.postId and l.siteid = p.siteid
            where l.siteid = {{site}}
            union 
            select p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews from last_ytd_before_hits l
            right join last_ytd_pageview_before p on l.postid=p.postId   and l.siteid = p.siteid
            where  p.siteid = {{site}}
            ),
            last_ytd_hits_pages_goal as (
            select l.*,g.min_pageviews,g.Min_CTA,
            case when (coalesce(l.hits,0)>=coalesce(g.Min_CTA,0) and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )
            then 1 else 0 end as gt_goal  from last_ytd_hits_pages l
            join  prod.goals g on g.date=l.publish_date and l.siteid = g.site_id
            where g.Site_ID  = {{site}}
            ),
            last_ytd_hits_pages_goal_before as (
            select l.*,g.min_pageviews,g.Min_CTA,
            case when (coalesce(l.hits,0)>=coalesce(g.Min_CTA,0) and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )
            then 1 else 0 end as gt_goal 
            from last_ytd_hits_pages_before l
            join  prod.goals g on g.date=l.publish_date and l.siteid = g.site_id
            where g.Site_ID  ={{site}}
            ),
            last_goal_achived_public_article as(
            select siteid,count(*) as total_publish_article,sum(gt_goal) as  value
            from last_ytd_hits_pages_goal
            where siteid = {{site}}
            group by 1
            ),
            last_goal_achived_public_article_before as(
            select siteid,count(*) as total_publish_article,sum(gt_goal) as value
            from last_ytd_hits_pages_goal_before
            where siteid = {{site}}
            group by 1
            )

            SELECT
            IFNULL((
            SELECT 
            JSON_OBJECT(
                "data", JSON_OBJECT(
                "label", "Artikler over på begge mål",
                "hint", "Artikler publiceret år til dato, der når begge mål ",
                "value", COALESCE(lg.value, 0),
                "change", COALESCE(ROUND(((lg.value - pg.value) / NULLIF(pg.value, 0)) * 100, 2), 0)
                ),
                "site", lg.siteid
            )
            FROM last_goal_achived_public_article lg
            LEFT JOIN last_goal_achived_public_article_before pg 
            ON lg.siteid = pg.siteid
        ),
        JSON_OBJECT(
            "data", JSON_OBJECT(
            "label", "Artikler over på begge mål",
            "hint", "Artikler publiceret år til dato, der når begge mål ",
            "value", 0,
            "change", 0
            ),
            "site", NULL
        )
        ) AS json_output;

            -- select 
            -- JSON_OBJECT(
            --         "data", JSON_OBJECT(
            --             "label", "Artikler over på begge mål",
            --             "hint", "Artikler publiceret år til dato, der når begge mål ",
            --             "value", lg.value,
            --             "change", coalesce(round(((lg.value-pg.value)/pg.value)*100,2),0)
            --         ),
            --         "site", lg.siteid
            --     ) AS json_output
            -- from last_goal_achived_public_article lg
            -- left join last_goal_achived_public_article_before pg on lg.siteid=pg.siteid;
        END

{% endmacro %}