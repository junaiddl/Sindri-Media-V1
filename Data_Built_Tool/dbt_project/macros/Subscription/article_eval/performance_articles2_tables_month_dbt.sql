{% macro subscription_performance_articles2_tables_month_dbt() %}        
        {% set site = var('site') %}
        {% set event = var('event_action') %}
        {% set json = var('click_val') %}
        {% set status = var('condition')['status'] %}
        {% set query = var('condition')['query'] %}
        {% set col_status = var('columns_to_add')['status'] %}
        {% set col_name  = var('columns_to_add')['columns_name'] %}
        {% set col_condition = var('columns_to_add')['condition'] %}




        CREATE PROCEDURE `performance_articles2_tables_month_dbt_{{site}}`()
        BEGIN


        WITH last_30days_post AS (
            SELECT siteid as siteid,ID,Title, userneeds as Categories, coalesce(tags, "") as tags,COALESCE(Categories, '') AS sektion, date as Date,link as url, DATE(Modified) as  updated
            {% if col_status %}
            ,{{col_condition}} {{col_name}}
            {% endif %}
            
            FROM prod.site_archive_post
            where date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) 
            {% if status %}
            {{query}}
            {% endif %}
            and siteid = {{site}} 
        ),
        last_30days_hits as(
            select 
                coalesce(e.siteid,{{site}}) as siteid,
                coalesce(e.PostID,ldp.Id) as postid,
                ldp.Categories,
                ldp.Title,
                ldp.date as publish_date,
                coalesce(sum(hits),0) as hits,
                ldp.url,
                ldp.updated,
                ldp.tags,
                ldp.sektion
                {% if col_status %}
                    ,ldp.{{col_name}}
                {% endif %}
            from  last_30days_post ldp
            left join prod.events e
            on e.postid=ldp.id and e.siteid = ldp.siteid
            and e.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) and e.siteid = {{site}}
            and e.Event_Action =  '{{event}}'
            group by 1,2,3,4,5,7,8,9,10
            {% if col_status %}
                    ,ldp.{{col_name}}
                {% endif %}
        ),
        last_30days_pageview as(
            select 
                coalesce(e.siteid,{{site}}) as siteid,
                coalesce(e.PostID,ldp.Id) as postid,
                ldp.Categories,
                ldp.Title,
                ldp.date as publish_date,
                coalesce(sum(unique_pageviews),0) as pageviews,
                ldp.url,
                ldp.updated,
                ldp.tags,
                ldp.sektion
                {% if col_status %}
                    ,ldp.{{col_name}}
                {% endif %}
            from  last_30days_post ldp
            join prod.pages e
            on e.postid=ldp.id and e.siteid = ldp.siteid
            and e.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) and e.siteid = {{site}}
            group by 1,2,3,4,5,7,8,9,10
            {% if col_status %}
                    ,ldp.{{col_name}}
                {% endif %}
        ),
        missing_post_in_pages as(
        select * from last_30days_pageview
        union
        select ldp.siteid, id as PostID ,ldp.Categories,ldp.Title,ldp.date as publish_date, 0  as pageviews,ldp.url,ldp.updated, ldp.tags,ldp.sektion  
        {% if col_status %}
                    ,ldp.{{col_name}}
                {% endif %}
        from last_30days_post ldp
        left join last_30days_pageview lp on lp.postid = ldp.id
        where lp.siteid is null
        ),
        last_30days_hits_pages as(
        select l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews,l.Categories,l.Title,l.url,l.updated,l.tags,l.sektion
        {% if col_status %}
                    ,l.{{col_name}}
                {% endif %}
        from last_30days_hits l
        left join missing_post_in_pages p on l.postid=p.postId and l.siteid = p.siteid
        where l.siteid = {{site}}
        union
        select p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews,p.Categories,p.Title,p.url,p.updated,p.tags,p.sektion  
        {% if col_status %}
                    ,p.{{col_name}}
                {% endif %}
        from last_30days_hits l
        right join missing_post_in_pages p on l.postid=p.postId and l.siteid = p.siteid
        where p.siteid = {{site}}
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
                case when (coalesce(l.hits,0)>=coalesce(g.Min_CTA,0) and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )
                then 1 else 0 end as gt_goal 
            {% if col_status %}
                    ,l.{{col_name}}
                {% endif %}
            from last_30days_hits_pages l
            join  prod.goals g on g.date=l.publish_date and g.site_id = l.siteid
            where  l.siteid = {{site}}
            group by 1,2,3,4,5,8,9,10,11,12,13,14,15,16
            {% if col_status %}
                    ,l.{{col_name}}
                {% endif %}
        )
        select 
        CONCAT('{"site":', siteid,',"data":{','"columns":{{json}},','"rows":',JSON_ARRAYAGG(
                JSON_OBJECT(
                    'id', postid,
                    'article', Title,
                    'category', Categories,
                        'sektion',sektion,
                        'tags', tags,
                        {% if col_status %}
                        '{{col_name}}', {{col_name}},
                    {% endif %}
                        'date', publish_date,
                        'updated', updated,
                        'url', url,
                   'brugerbehov', coalesce(pageviews,0),
                     'clicks',coalesce(hits,0)  
                ))
            ,'}}') AS json_data

        from last_30days_hits_pages_goal lg  where gt_goal=1 and siteid = {{site}};


        END

{% endmacro %}