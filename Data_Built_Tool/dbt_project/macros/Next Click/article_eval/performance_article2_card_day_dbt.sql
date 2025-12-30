{%macro next_click_performance_articles2_card_day_dbt() %}       
        {% set site = var('site') %}
        {% set event = var('event_action') %}
        {% set status = var('condition')['status'] %}
        {% set query = var('condition')['query'] %}


        CREATE  PROCEDURE `performance_articles2_card_day_dbt_{{site}}`()
        BEGIN

        WITH last_7days_post AS (
            SELECT siteid as siteid,ID, Date  as Date
            FROM prod.site_archive_post
            where date  between DATE_SUB(NOW(), INTERVAL 8 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) 
            {% if status %}
            {{query}}
            {% endif %}
            and siteid = {{site}}
        ),
        last_7_before_days_post AS (
            SELECT siteid as siteid,ID, Date as Date
            FROM prod.site_archive_post
            where date between DATE_SUB(NOW(), INTERVAL 15 DAY) and DATE_SUB(NOW(), INTERVAL  8 DAY) 
        {% if status %}
            {{query}}
            {% endif %}
            and siteid = {{site}}
        ),
        last_7days_hits as(
        select coalesce(e.siteid,ldp.siteid) as siteid,coalesce(e.PostID,ldp.id) as postid,ldp.date as publish_date,coalesce(sum(hits),0) as hits,e.Event_Action from  last_7days_post ldp
        left join prod.events e on e.postid=ldp.id   and e.siteid = ldp.siteid and e.date between DATE_SUB(NOW(), INTERVAL 8 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) and e.siteid = {{site}}
        and e.Event_Action='{{event}}'
        group by 1,2,3,5
        ),
        last_7days_before_hits as(
        select coalesce(e.siteid,ldp.siteid) as siteid,coalesce(e.PostID,ldp.id) as postid,ldp.date as publish_date,coalesce(sum(hits),0) as hits,e.Event_Action from  last_7_before_days_post ldp
        left join prod.events e
        on e.postid=ldp.id  and e.siteid = ldp.siteid
        and e.date  between DATE_SUB(NOW(), INTERVAL 15 DAY) and DATE_SUB(NOW(), INTERVAL  8 DAY) and e.siteid = {{site}}
        and e.Event_Action='{{event}}'
        group by 1,2,3,5
        ),
        last_7days_pageview as(
        select coalesce(e.siteid,ldp.siteid) as siteid,coalesce(e.PostID,ldp.id) as postid,ldp.date as publish_date,coalesce(sum(unique_pageviews),0) as pageviews from  last_7days_post ldp
        left join prod.pages e
        on e.postid=ldp.id   and e.siteid = ldp.siteid
        and e.date between DATE_SUB(NOW(), INTERVAL 8 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) and e.siteid = {{site}}
        group by 1,2,3
        ),
        last_7days_pageview_before as(
        select coalesce(e.siteid,ldp.siteid) as siteid,coalesce(e.PostID,ldp.id) as postid,ldp.date as publish_date,coalesce(sum(unique_pageviews),0) as pageviews  from  last_7_before_days_post ldp
        left join prod.pages e
        on e.postid=ldp.id 
        and e.date between DATE_SUB(NOW(), INTERVAL 15 DAY) and DATE_SUB(NOW(), INTERVAL  8 DAY) and e.siteid = {{site}}
        group by 1,2,3
        ),
        last_7days_hits_pages as(
        select l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews,l.Event_Action from last_7days_hits l
        left join last_7days_pageview p on l.postid=p.postId   and l.siteid = p.siteid
        where l.siteid = {{site}} -- and l.Event_Action is not null
        union 
        select p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews, l.Event_Action from last_7days_hits l
        right join last_7days_pageview p on l.postid=p.postId   and l.siteid = p.siteid
        where p.siteid = {{site}} -- and -- l.Event_Action is not null
        ),
        last_7days_hits_pages_before as(
        select l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews,l.Event_Action from last_7days_before_hits l
        left join last_7days_pageview_before p on l.postid=p.postId   and l.siteid = p.siteid
        where l.siteid = {{site}} -- and l.Event_Action is not null
        union 
        select p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews,l.Event_Action from last_7days_before_hits l
        right join last_7days_pageview_before p on l.postid=p.postId   and l.siteid = p.siteid
        where p.siteid = {{site}} -- and l.Event_Action is not null
        ),
        last_7days_hits_pages_goal as (
        select l.*,g.min_pageviews,g.Min_CTA,
        case when ((coalesce(coalesce(l.hits,0)/coalesce(l.pageviews,0)*100,0))>=coalesce(g.Min_CTA,0) and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )
        then 1 else 0 end as gt_goal
        from last_7days_hits_pages l
        join  prod.goals g on g.date=l.publish_date and g.site_id = l.siteid
        where  g.site_id = {{site}}
        ),
        last_7days_hits_pages_goal_before as (
        select l.*,g.min_pageviews,g.Min_CTA,
        case when ((coalesce(coalesce(l.hits,0)/coalesce(l.pageviews,0)*100,0))>=coalesce(g.Min_CTA,0) and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )
        then 1 else 0 end as gt_goal

        from last_7days_hits_pages_before l
        join  prod.goals g on g.date =l.publish_date and g.site_id = l.siteid
        where  g.site_id = {{site}}
        ),
        last_goal_achived_public_article as(
        select siteid,count(*) as total_publish_article,sum(gt_goal) as value
        from last_7days_hits_pages_goal
        where siteid = {{site}}
        group by 1
        ),
        last_goal_achived_public_article_before as(
        select siteid,count(*) as total_publish_article,sum(gt_goal) as value
        from last_7days_hits_pages_goal_before 
        where siteid = {{site}}
        group by 1
        )
        select 
        JSON_OBJECT(
                'data', JSON_OBJECT(
                    'label',  "Artikler over på begge mål",
                    'hint', "Artikler publiceret seneste 7 dage, der når begge mål",
                    'value', lg.value,
                    'change', coalesce(round(((lg.value-pg.value)/pg.value)*100,2),0)
                ),
                'site', lg.siteid
            ) AS json_output
        from last_goal_achived_public_article lg
        left join last_goal_achived_public_article_before pg on lg.siteid=pg.siteid
        where lg.siteid = {{site}};

            END

{% endmacro %}