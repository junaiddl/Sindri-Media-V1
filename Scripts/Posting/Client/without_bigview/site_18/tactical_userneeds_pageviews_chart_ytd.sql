


    














CREATE PROCEDURE `tactical_userneeds_pageviews_chart_ytd_dbt_18`()
BEGIN
    with 
    
		last_30_days_article_published as(
			SELECT siteid as siteid,id,  date as fdate,
			CASE
				WHEN userneeds REGEXP ".*Opdater mig.*" THEN "Opdater mig"
				WHEN userneeds REGEXP ".*Forbind mig.*" THEN "Forbind mig"
				WHEN userneeds REGEXP ".*Hjælp mig med at forstå.*" THEN "Hjælp mig med at forstå"
				WHEN userneeds REGEXP ".*Giv mig en fordel.*" THEN "Giv mig en fordel"
				WHEN userneeds REGEXP ".*Underhold mig.*" THEN "Underhold mig"
				WHEN userneeds REGEXP ".*Inspirer mig.*" THEN "Inspirer mig"
                ELSE 'others'
			END AS tags
			FROM prod.site_archive_post   
			WHERE date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)     
			AND siteid = 18
			
        ),
	 last_30_days_article_published_Agg as(
		select 
			siteid as siteid,
            tags,
            count(*) as agg 
		from  last_30_days_article_published
		where siteid = 18
		group by 1,2
		),
		cta_per_article as
		( 
			select  e.siteid as siteid,e.id as postid,e.tags,coalesce(sum(hits),0) as val from last_30_days_article_published  e
			left join prod.events  a on a.postid=e.id and a.Event_Action = 'Sales'
			where    e.siteid = 18
        	and a.date  between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
			group by 1,2,3
		),
	agg_cta_per_article as(
		select 
			siteid as siteid,
			tags,
			sum(val) as agg_sum 
		from  cta_per_article
		where siteid = 18
		group by 1,2
	),
	less_tg_data as(
		select 
		postid,l.tags ,val,min_cta
		,a.siteid,
		case when val<min_cta then 1 else 0 end as less_than_target
		,percent_rank() over ( order BY case when val>=min_cta then val-min_cta else 0 end) as percentile
		 from last_30_days_article_published l
		join cta_per_article a on l.siteid = a.siteid and l.id = a.postid and l.tags=a.tags
		join prod.goals g on a.siteid = g.site_id and g.Date = l.fdate
		 where date  between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
         and g.site_id = 18
		),
	counts as (
			select 
				siteid,
				tags,
				sum(less_than_target) as less_than_target_count,
				sum(case when percentile <= 1 and percentile >= 0.9 then 1 else 0 end) as top_10_count,
				sum(case when percentile >= 0 and percentile < 0.9 and less_than_target = 0 then 1 else 0 end) as approved_count
			from less_tg_data
			group by 1,2
		)
		,
		hits_by_tags as(
			select 
				siteid as siteid,
				tags,
				sum(less_than_target) as hits_tags,
				sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
				sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
		   from less_tg_data
		   where siteid = 18
		   group by 1,2
		),

		total_hits as(
			select 
				siteid as siteid ,
				sum(hits_tags) as total_tags_hit,
                sum(sum_by_top_10) as agg_by_top_10,
                sum(sum_by_approved) as agg_by_approved 
			from hits_by_tags
			where  siteid = 18
			group by 1
		),
		agg_data as(
			select 
				h.siteid as siteid ,
				h.tags,
				coalesce(hits_tags/total_tags_hit,0)*100 as less_than_target ,
				coalesce(sum_by_top_10/agg_by_top_10,0)*100 as top_10,
				coalesce(sum_by_approved/agg_by_approved,0)*100 as  approved
			from hits_by_tags h
			join total_hits t on h.siteid=t.siteid
			join last_30_days_article_published_Agg hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
			where h.siteid  = 18
		)
		,
		categories_d as (
			select 
				siteid as siteid ,
				'Artikler ift. tags og klik på køb' as label,
                'Artikler publiceret seneste 30 dage grupperet i tre kategorier: Artikler under mål | Artikler, der klarede mål | Top 10% bedste artikler ift. klik på køb. For hver kategori vises hvor stor andel der er publiceret indenfor hvert tag, så det er muligt at se forskelle på tværs af kategorierne.' as hint,
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
		less_than_target as less_than_target ,
		approved as approved ,
		 top_10 as top_10 
		 from  categories_d
		),
		json_data as (
		select siteid,label as lab,hint as h,cateogires as cat,CONCAT(
		'{"name": "Under mål", "data": [',cast(less_than_target as char),']}'
		,',{"name": "Godkendt" ,"data": [',cast(approved as char),']}',',
        {"name": "Top 10%" ,"data": [',cast(top_10 as char),']}') 
        as series from categories 
		)
    

    SELECT 
    IFNULL(
        (SELECT 
            CONCAT(
                '{',
                    '"site":', COALESCE(jd.siteid, 18), ',',
                    '"data": {',
                        '"label": "', COALESCE(jd.lab, 'Artikler ift. tags og klik på køb'), '",',
                        '"hint": "', COALESCE(jd.h, 'Artikler publiceret seneste 30 dage grupperet i tre kategorier: Artikler under mål | Artikler, der klarede mål | Top 10% bedste artikler ift. klik på køb. For hver kategori vises hvor stor andel der er publiceret indenfor hvert tag, så det er muligt at se forskelle på tværs af kategorierne.'), '",',
                        '"categories": [', COALESCE(jd.cat, '"Under mål","Over mål","Top 10%"'), '],',
                        '"series": [', COALESCE(jd.series, '[{"name": "Default Series","data":[0,0,0]}]'), '],',
                        '"defaultTitle": "', COALESCE('Brugerbehov', 'Default Title'), '",',
                        '"additional": [',
                        
                        ']',
                    '}',
                '}'
            ) 
        FROM json_data jd 
        ),
		'{
            "site": 18,
            "data": {
                "label": "Artikler ift. tags og klik på køb",
                "hint": "Artikler publiceret seneste 30 dage grupperet i tre kategorier: Artikler under mål | Artikler, der klarede mål | Top 10% bedste artikler ift. klik på køb. For hver kategori vises hvor stor andel der er publiceret indenfor hvert tag, så det er muligt at se forskelle på tværs af kategorierne.",
                "categories": [
                    "Opdater mig",
                    "Hjælp mig med at forstå",
                    "Giv mig en fordel",
                    "Underhold mig",
                    "Inspirer mig"
                ],
                "series": [
                    {
                        "name": "Under mål",
                        "data": [0.0,0.0,0.0,0.0,0.0]
                    },
                    {
                        "name": "Godkendt",
                        "data": [0.0,0.0,0.0,0.0,0.0]
                    },
                    {
                        "name": "Top 10%",
                        "data": [0.0,0.0,0.0,0.0,0.0]
                    }
                ],
                "defaultTitle": "Brugerbehov",
                "additional": []
            }
        }'
    ) AS json_data;
END


