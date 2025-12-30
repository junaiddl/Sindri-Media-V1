


    





CREATE PROCEDURE `tactical_userneeds_clicks_chart_month_dbt_18`()
BEGIN
with 
    last_ytd_article_published as(
        SELECT siteid as siteid,id, date as fdate   FROM prod.site_archive_post
        where 
        date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
        and siteid = 18
        
    ),
    last_ytd_article_published_Agg as(
        select siteid as siteid,count(*) as agg from  last_ytd_article_published
        where   siteid = 18
        group by 1
    ),
    cta_per_article as (
        select  e.siteid as siteid,e.postid,sum(hits) as val from prod.events e
        left join  last_ytd_article_published a on a.id=e.postid
        where Event_Action = 'Sales'  and e.siteid = 18
        and e.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
        group by 1,2
    ),
    agg_cta_per_article as(
        select siteid as siteid,sum(val) as agg_sum from  cta_per_article
        where   siteid = 18
        group by 1
    ),
    agg_data as(
        select 
        l.id,
        a.val,min_cta,
        percent_rank() over (ORDER BY case when val>=min_cta then val-min_cta else 0 end) as percentile
        ,case when coalesce (val,0)>=min_cta then val-min_cta else 0 end greater_than_target
        ,case when coalesce (val,0)<min_cta then 1 else 0 end as less_than_target
        ,percent_rank() over (ORDER BY case when coalesce (val,0)>=min_cta then coalesce (val,0) else 0 end) as percentile_click
        ,case when coalesce(val,0)>=min_cta then coalesce (val,0) else 0 end greater_than_target_click
        ,case when coalesce (val,0)<min_cta then coalesce (val,0) else 0 end as less_than_target_click
        ,l.siteid from last_ytd_article_published l
        left join cta_per_article a on l.siteid = a.siteid and l.id = a.postid
        left join prod.goals g on l.siteid = g.Site_ID and g.Date = l.fdate
        where g.date
        and date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
        and g.site_id = 18
    ),
    data_out as (
        select 
        a.siteid,
        'Artikler ift. klik på køb' as label,
        'Artikler publiceret seneste 30 dage grupperet i tre kategorier: Artikler under mål | Artikler, der klarede mål | Top 10% bedste artikler ift. klik på køb , En søjle viser hvor stor andel af artiklerne, der falder i hver gruppe. En søjle viser hvor stor en andel af samlede mængde klik på køb, der er skabt af den gruppe af artikler.' as hint,
        '"Under mål" , "Godkendt" , "Top 10%"' as categories,
        (sum(less_than_target)/agg)*100 as less_than_target,
        (sum(case when percentile<=1 and percentile>=0.9 then 1 else 0 end)/agg)*100 as top_10,
        (sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then 1 else 0 end)/agg)*100 as approved
        ,(sum(less_than_target_click)/agg_sum)*100 as less_than_target_click
        ,(sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then greater_than_target_click else 0 end)/agg_sum)*100 as approved_clicks
        ,(sum(case when percentile<=1 and percentile>=0.9 then val else 0 end)/agg_sum)*100 as top_10_clicks
        from agg_data a
        join last_ytd_article_published_Agg ag on ag.siteid=a.siteid
        join agg_cta_per_article ap on ap.siteid=a.siteid
        where   a.siteid = 18
        group by 1,2,3,4
    ),
    json_data AS (
        SELECT 
            COALESCE(siteid, 18) AS siteid, 
            COALESCE(label, 'Artikler ift. klik på køb') AS lab, 
            COALESCE(hint, 'Artikler publiceret seneste 30 dage grupperet i tre kategorier: Artikler under mål | Artikler, der klarede mål | Top 10% bedste artikler ift. klik på køb , En søjle viser hvor stor andel af artiklerne, der falder i hver gruppe. En søjle viser hvor stor en andel af samlede mængde klik på køb, der er skabt af den gruppe af artikler.') AS ht, 
            COALESCE(categories, '"Under mål" , "Godkendt" , "Top 10%"') AS cat,
            CONCAT(
                '{"name": "Andel artikler", "data": [', 
                COALESCE(CAST(less_than_target AS CHAR), '0'), ',',
                COALESCE(CAST(approved AS CHAR), '0'), ',',
                COALESCE(CAST(top_10 AS CHAR), '0'), ']},',
                '{"name": "Andel klik på køb", "data": [', 
                COALESCE(CAST(less_than_target_click AS CHAR), '0'), ',',
                COALESCE(CAST(approved_clicks AS CHAR), '0'), ',',
                COALESCE(CAST(top_10_clicks AS CHAR), '0'), ']}'
            ) AS series
        FROM data_out
    )
    SELECT 
    IFNULL(
        (SELECT 
            CONCAT(
                '{',
                '"site":', COALESCE(siteid, 18), ',',
                '"data":{"label":"', COALESCE(lab, 'Artikler ift. klik på køb'), '",',
                '"hint":"', COALESCE(ht, 'Artikler publiceret seneste 30 dage grupperet i tre kategorier: Artikler under mål | Artikler, der klarede mål | Top 10% bedste artikler ift. klik på køb , En søjle viser hvor stor andel af artiklerne, der falder i hver gruppe. En søjle viser hvor stor en andel af samlede mængde klik på køb, der er skabt af den gruppe af artikler.'), '",',
                '"categories":', '[', COALESCE(cat, '"Under mål","Over mål","Top 10%"'), '],',
                '"series":', '[', COALESCE(series, '[{"name": "Andel artikler","data":[0,0,0]},{"name": "Andel klik på køb","data":[0,0,0]}]'), ']}'
            ) 
        FROM json_data),
        '{"site":18,"data":{"label":"Artikler ift. klik på køb","hint":"Artikler publiceret seneste 30 dage grupperet i tre kategorier: Artikler under mål | Artikler, der klarede mål | Top 10% bedste artikler ift. klik på køb , En søjle viser hvor stor andel af artiklerne, der falder i hver gruppe. En søjle viser hvor stor en andel af samlede mængde klik på køb, der er skabt af den gruppe af artikler.","categories":["Under mål","Over mål","Top 10%"],"series":[{"name": "Andel artikler","data":[0,0,0]},{"name": "Andel klik på køb","data":[0,0,0]}]}}'
    ) AS json_data;
END

