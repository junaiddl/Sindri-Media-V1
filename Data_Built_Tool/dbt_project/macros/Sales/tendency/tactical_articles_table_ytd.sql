{% macro tactical_articles_table_ytd_dbt_sales() %}

{% set site = var('site') %}
{% set event_name = var('event_action') %}
{% set click_val = var('click_val') %}
{% set tag_filters_exclude = var('condition') %}
{% set table_modification = var('table_modification').table_modification_tendency %}

CREATE PROCEDURE `tactical_articles_table_ytd_dbt_{{site}}`()
BEGIN
    with last_30_days_article_published as(
        SELECT siteid as siteid,id,title as article,
        {{ table_modification['userneeds'] }} AS category, 
        {{ table_modification['tags'] }} AS tags,
        {{ table_modification['Categories'] }} AS sektion,
        link as url, DATE(Modified) as  updated,
        date as date  FROM prod.site_archive_post
        where 
        date  between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) 
        and siteid = {{site}} 
        {% if tag_filters_exclude.status %}
        {{tag_filters_exclude.query}}
        {% endif %}
    ),
    uniquepages as (
        select  e.siteid as siteid,e.postid,sum(unique_pageviews) as uniq_val from prod.pages e
        join  last_30_days_article_published a on a.id=e.postid
        where  e.siteid = {{site}}
        and e.date  between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) 
        group by 1,2
    ),
    cta_per_article as (
        select  e.siteid as siteid,e.postid,sum(hits) as val_hits from prod.events e
        left join  last_30_days_article_published a on a.id=e.postid
        where Event_Action = '{{event_name}}' and  e.siteid = {{site}}
        and e.date  between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) 
        group by 1,2
    )

    select 
    CONCAT('{"site":',  l.siteid,',"data":{','"columns": {{click_val}},','"rows":',JSON_ARRAYAGG(
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
                'clicks',coalesce(val_hits,0)
            ))
        ,'}}'
    ) AS json_data
    from last_30_days_article_published l
    left join uniquepages up
    on l.siteid = up.siteid and l.id = up.postid
    left join cta_per_article ca
    on l.siteid = ca.siteid and l.id = ca.postid 
    where  l.siteid = {{site}};
END
{% endmacro %}