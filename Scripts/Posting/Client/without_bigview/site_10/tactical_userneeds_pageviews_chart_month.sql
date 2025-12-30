


    














CREATE PROCEDURE `tactical_userneeds_pageviews_chart_month_dbt_10`()
BEGIN
    with 
    
		last_30_days_article_published as(
			SELECT siteid as siteid,id,  date as fdate,
			CASE
				WHEN Categories REGEXP ".*Fodsundhed og samfund.*" THEN "Fodsundhed og samfund"
				WHEN Categories REGEXP ".*Forebyggelse.*" THEN "Forebyggelse"
				WHEN Categories REGEXP ".*Forskning og viden.*" THEN "Forskning og viden"
				WHEN Categories REGEXP ".*Praksis.*" THEN "Praksis"
                ELSE 'others'
			END AS tags
			FROM prod.site_archive_post   
			WHERE date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)         
			AND siteid = 10
			
        ),
	 last_30_days_article_published_Agg as(
		select 
			siteid as siteid,
            tags,
            count(*) as agg 
		from  last_30_days_article_published
		where siteid = 10
		group by 1,2
		),
	  agg_next_click_per_article as 
      (
		select  
			e.siteid as siteid,
            e.id as postid,
            e.tags,
            coalesce(sum(hits),0) as val
		from last_30_days_article_published  e
		left join prod.events  a on a.postid=e.id and a.siteid = e.siteid and a.event_action = 'Newsletter'
		where    e.siteid = 10
        and a.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
		group by 1,2,3
		),
	agg_pages_per_article as 
      (
		select  
			e.siteid as siteid,
            e.id as postid,
            e.tags,
			coalesce(sum(unique_pageviews),0) as page_view 
		from last_30_days_article_published  e
		left join prod.pages p on p.postid=e.id and p.siteid =e.siteid
		where    e.siteid = 10
        and p.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
		group by 1,2,3
		)  ,
	agg_total as (
			select
			e.siteid as siteid,
			e.id as postid,
			e.tags as tags,
			p.page_view as page_view,
			a.val as val
			from last_30_days_article_published  e
			left join agg_pages_per_article p on p.postid=e.id and p.siteid =e.siteid
			left join agg_next_click_per_article  a on a.postid=e.id and a.siteid = e.siteid
        ) ,
		cta_per_article as
		( 
			select  e.siteid as siteid, e.postid, e.tags,
			round(coalesce((coalesce(val,0)/coalesce(page_view,1)),0.0)*100.0,2) as val from agg_total  e
			where  e.siteid = 10
			group by 1,2,3

		),
	agg_cta_per_article as(
		select siteid as siteid,tags,sum(val) as agg_sum from  cta_per_article
		where siteid = 10
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
		 where date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
         and g.site_id = 10
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
		   where siteid = 10
		   group by 1,2
		),

		total_hits as(
			select 
				siteid as siteid ,
				sum(less_than_target_count) as total_tags_hit,
                sum(top_10_count) as agg_by_top_10,
                sum(approved_count) as agg_by_approved 
			from counts
			where  siteid = 10
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
			where h.siteid  = 10
		)
		,
		categories_d as (
			select 
				siteid as siteid ,
				'Artikler ift. tags og sign-ups mål' as label,
                'Artikler publiceret seneste 30 dage grupperet i tre kategorier: Artikler under sign-ups mål | Artikler, der nåede sign-ups mål | Top 10% bedste artikler ift. sign-ups. For hver kategori vises hvor stor andel der er publiceret indenfor hvert tag, så det er muligt at se forskelle på tværs af kategorierne.' as hint,
				GROUP_CONCAT(tags ORDER BY FIELD(tags, 'Fodsundhed og samfund', 'Forebyggelse', 'Forskning og viden', 'Praksis', 'others' ) SEPARATOR ',') AS cateogires,
				GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, 'Fodsundhed og samfund', 'Forebyggelse', 'Forskning og viden', 'Praksis', 'others' ) SEPARATOR ',') AS less_than_target,
			GROUP_CONCAT(approved ORDER BY FIELD(tags, 'Fodsundhed og samfund', 'Forebyggelse', 'Forskning og viden', 'Praksis', 'others' ) SEPARATOR ',') AS approved,
			GROUP_CONCAT(top_10 ORDER BY FIELD(tags, 'Fodsundhed og samfund', 'Forebyggelse', 'Forskning og viden', 'Praksis', 'others' ) SEPARATOR ',') AS top_10
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
		,',{"name": "Over mål" ,"data": [',cast(approved as char),']}',',
        {"name": "Top 10%" ,"data": [',cast(top_10 as char),']}') 
        as series from categories 
		), 
    
		last_30_days_article_published_second as(
			SELECT siteid as siteid,id,  date as fdate,
			CASE
				WHEN Tags REGEXP ".*Hjælp mig med at forstå.*" THEN "Hjælp mig med at forstå"
				WHEN Tags REGEXP ".*Inspirer mig.*" THEN "Inspirer mig"
				WHEN Tags REGEXP ".*Giv mig en fordel.*" THEN "Giv mig en fordel"
                ELSE 'others'
			END AS tags
			FROM prod.site_archive_post   
			WHERE date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)         
			AND siteid = 10
			
        ),
	 last_30_days_article_published_Agg_second as(
		select 
			siteid as siteid,
            tags,
            count(*) as agg 
		from  last_30_days_article_published_second
		where siteid = 10
		group by 1,2
		),
	  agg_next_click_per_article_second as 
      (
		select  
			e.siteid as siteid,
            e.id as postid,
            e.tags,
            coalesce(sum(hits),0) as val
		from last_30_days_article_published_second  e
		left join prod.events  a on a.postid=e.id and a.siteid = e.siteid and a.event_action = 'Newsletter'
		where    e.siteid = 10
        and a.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
		group by 1,2,3
		),
	agg_pages_per_article_second as 
      (
		select  
			e.siteid as siteid,
            e.id as postid,
            e.tags,
			coalesce(sum(unique_pageviews),0) as page_view 
		from last_30_days_article_published_second  e
		left join prod.pages p on p.postid=e.id and p.siteid =e.siteid
		where    e.siteid = 10
        and p.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
		group by 1,2,3
		)  ,
	agg_total_second as (
			select
			e.siteid as siteid,
			e.id as postid,
			e.tags as tags,
			p.page_view as page_view,
			a.val as val
			from last_30_days_article_published_second  e
			left join agg_pages_per_article_second p on p.postid=e.id and p.siteid =e.siteid
			left join agg_next_click_per_article_second  a on a.postid=e.id and a.siteid = e.siteid
        ) ,
		cta_per_article_second as
		( 
			select  e.siteid as siteid, e.postid, e.tags,
			round(coalesce((coalesce(val,0)/coalesce(page_view,1)),0.0)*100.0,2) as val from agg_total_second  e
			where  e.siteid = 10
			group by 1,2,3

		),
	agg_cta_per_article_second as(
		select siteid as siteid,tags,sum(val) as agg_sum from  cta_per_article_second
		where siteid = 10
		group by 1,2
	),
	less_tg_data_second as(
		select 
		postid,l.tags ,val,min_cta
		,a.siteid,
		case when val<min_cta then 1 else 0 end as less_than_target
		,percent_rank() over ( order BY case when val>=min_cta then val-min_cta else 0 end) as percentile
		 from last_30_days_article_published_second l
		join cta_per_article_second a on l.siteid = a.siteid and l.id = a.postid and l.tags=a.tags
		join prod.goals g on a.siteid = g.site_id and g.Date = l.fdate
		 where date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
         and g.site_id = 10
		),
	counts_second as (
			select 
				siteid,
				tags,
				sum(less_than_target) as less_than_target_count,
				sum(case when percentile <= 1 and percentile >= 0.9 then 1 else 0 end) as top_10_count,
				sum(case when percentile >= 0 and percentile < 0.9 and less_than_target = 0 then 1 else 0 end) as approved_count
			from less_tg_data_second
			group by 1,2
		)
		,
		hits_by_tags_second as(
			select 
				siteid as siteid,
				tags,
				sum(less_than_target) as hits_tags,
				sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
				sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
		   from less_tg_data_second
		   where siteid = 10
		   group by 1,2
		),

		total_hits_second as(
			select 
				siteid as siteid ,
				sum(less_than_target_count) as total_tags_hit,
                sum(top_10_count) as agg_by_top_10,
                sum(approved_count) as agg_by_approved 
			from counts
			where  siteid = 10
			group by 1
		),
		agg_data_second as(
			select 
				h.siteid as siteid ,
				h.tags,
				coalesce(less_than_target_count/total_tags_hit,0)*100 as less_than_target ,
				coalesce(top_10_count/agg_by_top_10,0)*100 as top_10,
				coalesce(approved_count/agg_by_approved,0)*100 as  approved
			from counts_second h
			join total_hits_second t on h.siteid=t.siteid
			join last_30_days_article_published_Agg_second hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
			where h.siteid  = 10
		)
		,
		categories_d_second as (
			select 
				siteid as siteid ,
				'Artikler ift. tags og sign-ups mål' as label,
                'Artikler publiceret seneste 30 dage grupperet i tre kategorier: Artikler under sign-ups mål | Artikler, der nåede sign-ups mål | Top 10% bedste artikler ift. sign-ups. For hver kategori vises hvor stor andel der er publiceret indenfor hvert tag, så det er muligt at se forskelle på tværs af kategorierne.' as hint,
				GROUP_CONCAT(tags ORDER BY FIELD(tags, 'Hjælp mig med at forstå', 'Inspirer mig', 'Giv mig en fordel', 'others' ) SEPARATOR ',') AS cateogires,
				GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, 'Hjælp mig med at forstå', 'Inspirer mig', 'Giv mig en fordel', 'others' ) SEPARATOR ',') AS less_than_target,
			GROUP_CONCAT(approved ORDER BY FIELD(tags, 'Hjælp mig med at forstå', 'Inspirer mig', 'Giv mig en fordel', 'others' ) SEPARATOR ',') AS approved,
			GROUP_CONCAT(top_10 ORDER BY FIELD(tags, 'Hjælp mig med at forstå', 'Inspirer mig', 'Giv mig en fordel', 'others' ) SEPARATOR ',') AS top_10
		from agg_data_second
		group by 1,2,3
		),
        categories_second as (
		select siteid as siteid ,label as label, hint as hint,  
		 CONCAT('"', REPLACE(cateogires, ',', '","'), '"') AS  cateogires,
		less_than_target as less_than_target ,
		approved as approved ,
		 top_10 as top_10 
		 from  categories_d_second
		),
		json_data_second as (
		select siteid,label as lab,hint as h,cateogires as cat,CONCAT(
		'{"name": "Under mål", "data": [',cast(less_than_target as char),']}'
		,',{"name": "Over mål" ,"data": [',cast(approved as char),']}',',
        {"name": "Top 10%" ,"data": [',cast(top_10 as char),']}') 
        as series from categories_second 
		), 
    
		last_30_days_article_published_third as(
			SELECT siteid as siteid,id,  date as fdate,
			CASE
				WHEN tags_r REGEXP ".*Long read.*" THEN "Long read"
				WHEN tags_r REGEXP ".*Kort og godt.*" THEN "Kort og godt"
				WHEN tags_r REGEXP ".*Q&amp.*" THEN "Q&A"
				WHEN tags_r REGEXP ".*Best Practice.*" THEN "Best Practice"
				WHEN tags_r REGEXP ".*Viden og forskning.*" THEN "Viden og forskning"
                ELSE 'others'
			END AS tags
			FROM prod.site_archive_post   
			WHERE date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)         
			AND siteid = 10
			
        ),
	 last_30_days_article_published_Agg_third as(
		select 
			siteid as siteid,
            tags,
            count(*) as agg 
		from  last_30_days_article_published_third
		where siteid = 10
		group by 1,2
		),
	  agg_next_click_per_article_third as 
      (
		select  
			e.siteid as siteid,
            e.id as postid,
            e.tags,
            coalesce(sum(hits),0) as val
		from last_30_days_article_published_third  e
		left join prod.events  a on a.postid=e.id and a.siteid = e.siteid and a.event_action = 'Newsletter'
		where    e.siteid = 10
        and a.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
		group by 1,2,3
		),
	agg_pages_per_article_third as 
      (
		select  
			e.siteid as siteid,
            e.id as postid,
            e.tags,
			coalesce(sum(unique_pageviews),0) as page_view 
		from last_30_days_article_published_third  e
		left join prod.pages p on p.postid=e.id and p.siteid =e.siteid
		where    e.siteid = 10
        and p.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
		group by 1,2,3
		)  ,
	agg_total_third as (
			select
			e.siteid as siteid,
			e.id as postid,
			e.tags as tags,
			p.page_view as page_view,
			a.val as val
			from last_30_days_article_published_third  e
			left join agg_pages_per_article_third p on p.postid=e.id and p.siteid =e.siteid
			left join agg_next_click_per_article_third  a on a.postid=e.id and a.siteid = e.siteid
        ) ,
		cta_per_article_third as
		( 
			select  e.siteid as siteid, e.postid, e.tags,
			round(coalesce((coalesce(val,0)/coalesce(page_view,1)),0.0)*100.0,2) as val from agg_total_third  e
			where  e.siteid = 10
			group by 1,2,3

		),
	agg_cta_per_article_third as(
		select siteid as siteid,tags,sum(val) as agg_sum from  cta_per_article_third
		where siteid = 10
		group by 1,2
	),
	less_tg_data_third as(
		select 
		postid,l.tags ,val,min_cta
		,a.siteid,
		case when val<min_cta then 1 else 0 end as less_than_target
		,percent_rank() over ( order BY case when val>=min_cta then val-min_cta else 0 end) as percentile
		 from last_30_days_article_published_third l
		join cta_per_article_third a on l.siteid = a.siteid and l.id = a.postid and l.tags=a.tags
		join prod.goals g on a.siteid = g.site_id and g.Date = l.fdate
		 where date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
         and g.site_id = 10
		),
	counts_third as (
			select 
				siteid,
				tags,
				sum(less_than_target) as less_than_target_count,
				sum(case when percentile <= 1 and percentile >= 0.9 then 1 else 0 end) as top_10_count,
				sum(case when percentile >= 0 and percentile < 0.9 and less_than_target = 0 then 1 else 0 end) as approved_count
			from less_tg_data_third
			group by 1,2
		)
		,
		hits_by_tags_third as(
			select 
				siteid as siteid,
				tags,
				sum(less_than_target) as hits_tags,
				sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
				sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
		   from less_tg_data_third
		   where siteid = 10
		   group by 1,2
		),

		total_hits_third as(
			select 
				siteid as siteid ,
				sum(less_than_target_count) as total_tags_hit,
                sum(top_10_count) as agg_by_top_10,
                sum(approved_count) as agg_by_approved 
			from counts
			where  siteid = 10
			group by 1
		),
		agg_data_third as(
			select 
				h.siteid as siteid ,
				h.tags,
				coalesce(less_than_target_count/total_tags_hit,0)*100 as less_than_target ,
				coalesce(top_10_count/agg_by_top_10,0)*100 as top_10,
				coalesce(approved_count/agg_by_approved,0)*100 as  approved
			from counts_third h
			join total_hits_third t on h.siteid=t.siteid
			join last_30_days_article_published_Agg_third hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
			where h.siteid  = 10
		)
		,
		categories_d_third as (
			select 
				siteid as siteid ,
				'Artikler ift. tags og sign-ups mål' as label,
                'Artikler publiceret seneste 30 dage grupperet i tre kategorier: Artikler under sign-ups mål | Artikler, der nåede sign-ups mål | Top 10% bedste artikler ift. sign-ups. For hver kategori vises hvor stor andel der er publiceret indenfor hvert tag, så det er muligt at se forskelle på tværs af kategorierne.' as hint,
				GROUP_CONCAT(tags ORDER BY FIELD(tags, 'Long read', 'Kort og godt', 'Q&A', 'Best Practice', 'Viden og forskning', 'others' ) SEPARATOR ',') AS cateogires,
				GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, 'Long read', 'Kort og godt', 'Q&A', 'Best Practice', 'Viden og forskning', 'others' ) SEPARATOR ',') AS less_than_target,
			GROUP_CONCAT(approved ORDER BY FIELD(tags, 'Long read', 'Kort og godt', 'Q&A', 'Best Practice', 'Viden og forskning', 'others' ) SEPARATOR ',') AS approved,
			GROUP_CONCAT(top_10 ORDER BY FIELD(tags, 'Long read', 'Kort og godt', 'Q&A', 'Best Practice', 'Viden og forskning', 'others' ) SEPARATOR ',') AS top_10
		from agg_data_third
		group by 1,2,3
		),
        categories_third as (
		select siteid as siteid ,label as label, hint as hint,  
		 CONCAT('"', REPLACE(cateogires, ',', '","'), '"') AS  cateogires,
		less_than_target as less_than_target ,
		approved as approved ,
		 top_10 as top_10 
		 from  categories_d_third
		),
		json_data_third as (
		select siteid,label as lab,hint as h,cateogires as cat,CONCAT(
		'{"name": "Under mål", "data": [',cast(less_than_target as char),']}'
		,',{"name": "Over mål" ,"data": [',cast(approved as char),']}',',
        {"name": "Top 10%" ,"data": [',cast(top_10 as char),']}') 
        as series from categories_third 
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
                    '"defaultTitle": "Kategorier",',
                    '"additional": [',
                    
                    '{',
                        '"title": "Brugerbehov",',
                        '"data": {',
                            '"label": "', json_data_second.lab, '",',
                            '"categories": [', json_data_second.cat, '],',
                            '"series": [', json_data_second.series, ']',
                        '}',
                    '}'
                    ','
                    
                    '{',
                        '"title": "Formater",',
                        '"data": {',
                            '"label": "', json_data_third.lab, '",',
                            '"categories": [', json_data_third.cat, '],',
                            '"series": [', json_data_third.series, ']',
                        '}',
                    '}'
                    
                    
                    ']',
                '}',
            '}'
        ) AS json_data
    FROM json_data jd
    
    CROSS JOIN json_data_second
    
    CROSS JOIN json_data_third
    ;
END


