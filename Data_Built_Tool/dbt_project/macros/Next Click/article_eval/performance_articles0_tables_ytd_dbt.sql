{% macro next_click_performance_articles0_tables_ytd_dbt() %}        
        {% set site = var('site') %}
        {% set event = var('event_action') %}
        {% set json = var('click_val') %}
        {% set status = var('condition')['status'] %}
        {% set query = var('condition')['query'] %}
        {% set col_status = var('columns_to_add')['status'] %}
        {% set col_name  = var('columns_to_add')['columns_name'] %}
        {% set col_condition = var('columns_to_add')['condition'] %}
        {% set table_modification = var('table_modification').table_modification_tendency %}


        CREATE  PROCEDURE `performance_articles0_tables_ytd_dbt_{{site}}`()
        BEGIN

        WITH last_ytd_post AS (
            SELECT siteid as siteid,ID,Title, 
        {{ table_modification['userneeds'] }} AS Categories, 
        {{ table_modification['tags'] }} AS tags,
        {{ table_modification['Categories'] }} AS sektion,
            date as Date,link as url, DATE(Modified) as  updated
            {% if col_status %}
            ,{{col_condition}} {{col_name}}
            {% endif %}
            FROM prod.site_archive_post
            where date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) 
        {% if status %}
            {{query}}
            {% endif %}
            and siteid = {{site}} 

            ),

        last_ytd_hits as (
            SELECT 
                COALESCE(e.siteid, {{site}}) AS siteid,
                COALESCE(e.PostID, ldp.Id) AS postid,
                ldp.Categories,
                MAX(ldp.Title) AS Title,
                ldp.date AS publish_date,
                COALESCE(SUM(hits), 0) AS hits,
                ldp.url,
                ldp.updated,
                ldp.tags,
                ldp.sektion
                {% if col_status %}
                    ,ldp.{{col_name}}
                {% endif %}
            FROM   
                last_ytd_post ldp
                LEFT JOIN prod.events e ON e.postid = ldp.id AND e.siteid = ldp.siteid AND e.date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY) AND e.siteid = {{site}} AND e.Event_Action =  '{{event}}'
            GROUP BY 1, 2, 3, 5, 7, 8, 9,10
            {% if col_status %}
                    ,ldp.{{col_name}}
            {% endif %}
        ),

        last_ytd_pageview as (
            SELECT 
                COALESCE(e.siteid, {{site}}) AS siteid,
                COALESCE(e.PostID, ldp.Id) AS postid,
                ldp.Categories,
                MAX(ldp.Title) AS Title,
                ldp.date AS publish_date,
                COALESCE(SUM(unique_pageviews), 0) AS pageviews,
                ldp.url,
                ldp.updated,
                ldp.tags,
                ldp.sektion
                {% if col_status %}
                    ,ldp.{{col_name}}
            {% endif %}
            FROM 
                last_ytd_post ldp
                LEFT JOIN prod.pages e ON e.postid = ldp.id AND e.siteid = ldp.siteid AND e.date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY) AND e.siteid = {{site}}
            GROUP BY 1, 2, 3, 5, 7, 8, 9,10
            {% if col_status %}
                    ,ldp.{{col_name}}
            {% endif %}
        ),


        last_ytd_hits_pages as(
        select l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews,l.Categories,l.Title,l.url, l.updated,l.tags, l.sektion 
        {% if col_status %}
            ,l.{{col_name}}
        {% endif %}
        from last_ytd_hits l
        left join last_ytd_pageview p on l.postid=p.postId  and l.siteid=p.siteid
        where l.siteid = {{site}}
        union 
        select p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews,p.Categories,p.Title ,p.url, p.updated, p.tags,p.sektion 
        {% if col_status %}
            ,p.{{col_name}}
        {% endif %}
        from last_ytd_hits l
        right join last_ytd_pageview p on l.postid=p.postId and p.siteid=l.siteid
        where p.siteid = {{site}}
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
        case when (coalesce((coalesce(l.hits,0)/coalesce(l.pageviews,0)*100),0)<coalesce(g.Min_CTA,0) and coalesce(l.pageviews,0)<coalesce(g.Min_pageviews,0) ) then 1 else 0 end as gt_goal
        {% if col_status %}
                    ,l.{{col_name}}
            {% endif %}
                from last_ytd_hits_pages l
            join  prod.goals g on g.date=l.publish_date and g.site_id = l.siteid
            where  g.site_id = {{site}}
            GROUP BY   1,2,3,4,5,8,9,10,11,12,13, 14,15
            {% if col_status %}
                    ,l.{{col_name}}
            {% endif %}
        )

        select 
        CONCAT('{"site":', siteid,',"data":{','"columns":{{json}},','"rows":',JSON_ARRAYAGG(
                JSON_OBJECT(
                    'id', postid,
                    'article', Title,
                    'category', coalesce(Categories, ''),
                        'sektion',coalesce(sektion , ''),
                        'tags', coalesce(tags, ''),
                        {% if col_status %}
                        '{{col_name}}', {{col_name}},
                    {% endif %}
                        'date', publish_date,
                        'updated', updated,
                        'url', url,
                    'brugerbehov', coalesce(pageviews,0),
                    'clicks',ROUND(coalesce(lg.s_hits, 0) / coalesce(lg.s_pageviews, 0), 3)*100  
                ))
            ,'}}') AS json_data


        from last_ytd_hits_pages_goal lg where gt_goal  =1 and siteid = {{site}};


                END
{% endmacro %}