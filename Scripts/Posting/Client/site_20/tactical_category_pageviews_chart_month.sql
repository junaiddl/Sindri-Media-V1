


    














CREATE PROCEDURE `tactical_category_pageviews_chart_month_dbt_20`()
BEGIN
		with cms_data as (
            SELECT siteid, id, date as fdate, Categories, userneeds, tags
            FROM prod.site_archive_post
            WHERE
            date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY)
            AND DATE_SUB(NOW(), INTERVAL 1 DAY)
            AND siteid = 20
            
        ),
        
            last_30_days_article_published_without_others as(
                SELECT siteid, id, fdate, "Opdater mig" AS tags
                FROM cms_data
                WHERE userneeds REGEXP ".*Opdater mig.*"
                UNION ALL
                
                SELECT siteid, id, fdate, "Forbind mig" AS tags
                FROM cms_data
                WHERE userneeds REGEXP ".*Forbind mig.*"
                UNION ALL
                
                SELECT siteid, id, fdate, "Hjælp mig med at forstå" AS tags
                FROM cms_data
                WHERE userneeds REGEXP ".*Hjælp mig med at forstå.*"
                UNION ALL
                
                SELECT siteid, id, fdate, "Giv mig en fordel" AS tags
                FROM cms_data
                WHERE userneeds REGEXP ".*Giv mig en fordel.*"
                UNION ALL
                
                SELECT siteid, id, fdate, "Underhold mig" AS tags
                FROM cms_data
                WHERE userneeds REGEXP ".*Underhold mig.*"
                UNION ALL
                
                SELECT siteid, id, fdate, "Inspirer mig" AS tags
                FROM cms_data
                WHERE userneeds REGEXP ".*Inspirer mig.*"   
            ),
            last_30_days_article_published as (
                SELECT a.siteid, a.id, a.fdate, 'others' as Tags
                FROM cms_data a
                LEFT JOIN last_30_days_article_published_without_others b
                ON a.siteid = b.siteid AND a.id = b.id
                WHERE b.siteid IS NULL

                UNION ALL

                SELECT * FROM last_30_days_article_published_without_others
            ),
            cta_per_article as (
                select  e.siteid as siteid,e.postid,a.tags,sum(unique_pageviews) as val from last_30_days_article_published a
                left join  prod.pages e on a.id=e.postid
                and e.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
                group by 1,2,3
            ),
            agg_cta_per_article as(
                select siteid as siteid,tags, coalesce(sum(val), 0) as agg_sum from  cta_per_article
                where siteid = 20
                group by 1,2
            ),
            last_30_days_article_published_Agg as(
                select siteid as siteid,tags,count(*) as agg from  cta_per_article
                where siteid = 20
                group by 1,2
            ),
            less_tg_data as(
                select 
                postid,l.tags ,val,Min_pageviews
                ,a.siteid,
                case when val<Min_pageviews then 1 else 0 end as less_than_target
                ,percent_rank() over ( order BY case when val>=Min_pageviews then val-Min_pageviews else 0 end) as percentile
                from last_30_days_article_published l
                join cta_per_article a on l.siteid = a.siteid and l.id = a.postid and l.tags=a.tags
                join prod.goals g on a.siteid = g.site_id and g.Date = l.fdate
                where date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
                and a.siteid = 20
            ),
            counts  as (
			select 
				siteid,
				tags,
				sum(less_than_target) as less_than_target_count,
				sum(case when percentile <= 1 and percentile >= 0.9 then 1 else 0 end) as top_10_count,
				sum(case when percentile >= 0 and percentile < 0.9 and less_than_target = 0 then 1 else 0 end) as approved_count
			from less_tg_data
			group by 1,2
            ),
            hits_by_tags as(
                select 
                    siteid as siteid,
                    tags,
                    sum(less_than_target) as hits_tags,
                    sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
                    sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
            from less_tg_data
            where siteid = 20
            group by 1,2
            ),
            total_hits as(
                select 
                    siteid as siteid ,
                    sum(less_than_target_count) as total_tags_hit,
                    sum(top_10_count) as agg_by_top_10,
                    sum(approved_count) as agg_by_approved 
                from counts
                where  siteid = 20
                group by 1
            ),
            agg_data as(
                select 
                    h.siteid as siteid ,
                    h.tags,
                    coalesce(less_than_target_count/total_tags_hit,0)*100 as less_than_target ,
                    coalesce(top_10_count/agg_by_top_10,0)*100 as top_10,
                    coalesce(approved_count/agg_by_approved,0)*100 as  approved
                from counts h
                join total_hits t on h.siteid=t.siteid
                join last_30_days_article_published_Agg hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
                where h.siteid  = 20
            ),
            categories_d as (
                select 
                    siteid as siteid ,
                    'Artikler ift. tags og sidevisningsmål' as label,
                    'Artikler publiceret seneste 30 dage grupperet i tre kategorier: Artikler under sidevisningsmål | Artikler, der klarede mål | Top 10% bedste artikler ift. sidevisninger. For hver kategori vises hvor stor andel der er publiceret indenfor hvert tag, så det er muligt at se forskelle på tværs af kategorierne.' as hint,
                    GROUP_CONCAT(tags ORDER BY FIELD(tags, 'Opdater mig','Forbind mig','Hjælp mig med at forstå','Giv mig en fordel', 'Underhold mig', 'Inspirer mig', 'others' ) SEPARATOR ',') AS cateogires,
                    GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, 'Opdater mig','Forbind mig','Hjælp mig med at forstå','Giv mig en fordel', 'Underhold mig', 'Inspirer mig', 'others' ) SEPARATOR ',') AS less_than_target,
                    GROUP_CONCAT(approved ORDER BY FIELD(tags, 'Opdater mig','Forbind mig','Hjælp mig med at forstå','Giv mig en fordel', 'Underhold mig', 'Inspirer mig', 'others' ) SEPARATOR ',') AS approved,
                    GROUP_CONCAT(top_10 ORDER BY FIELD(tags, 'Opdater mig','Forbind mig','Hjælp mig med at forstå','Giv mig en fordel', 'Underhold mig', 'Inspirer mig', 'others' ) SEPARATOR ',') AS top_10
                from agg_data
                group by 1,2,3
            ),
            categories as (
            select siteid as siteid ,label as label, hint as hint,  
		    CONCAT('"', REPLACE(cateogires, ',', '","'), '"') AS  cateogires,
            less_than_target as less_than_target,
            approved as approved,
            top_10 as top_10 
            from  categories_d
            ),
            json_data as (
            select siteid,label as lab,hint as h,cateogires as cat,CONCAT(
            '{"name": "Under mål", "data": [',cast(less_than_target as char),']}'
            ,',{"name": "Over mål" ,"data": [',cast(approved as char),']}',',
            {"name": "Top 10%" ,"data": [',cast(top_10 as char),']}') 
            as series from categories 
            )
        
        
SELECT 
        CONCAT(
            '{',
                '"site":', jd.siteid, ',',
                '"data": {',
                    '"label": "', jd.lab, '",',
                    '"hint": "', jd.h, '",',
                    '"categories": [', jd.cat, '],',
                    '"series": [', jd.series, '],',
                    '"defaultTitle": "Brugerbehov",',
                    '"additional": [',
                    
                    ']',
                '}',
            '}'
        ) AS json_data
    FROM json_data jd
    ;
END


