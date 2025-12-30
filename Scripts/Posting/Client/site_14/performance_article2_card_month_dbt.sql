


    		
		
		
		
		


		CREATE PROCEDURE `performance_articles2_card_month_dbt_14`()
		BEGIN
		WITH last_30days_post AS (
			SELECT siteid as siteid,ID, date as Date
			FROM prod.site_archive_post
			where date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) 
			
			AND categories <> 'Nyhedsoverblik'
			
			and siteid = 14
		),
		last_30_before_days_post AS (
			SELECT siteid as siteid,ID, date as Date
			FROM prod.site_archive_post
			where date  between DATE_SUB(NOW(), INTERVAL 61 DAY ) and DATE_SUB(NOW(), INTERVAL  31 DAY) 
			
			AND categories <> 'Nyhedsoverblik'
			
			and siteid = 14
		),
		last_30days_hits as(
		select coalesce(e.siteid,ldp.siteid) as siteid,coalesce(e.PostID,ldp.id) as postid,ldp.date as publish_date,coalesce(sum(hits),0) as hits,e.Event_Action
		from  last_30days_post ldp
		left join prod.events e
		on e.postid=ldp.id and e.siteid = ldp.siteid
		and e.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) and e.siteid = 14
		and e.Event_Action = "Next Click"
		group by 1,2,3,5
		),
		last_30days_before_hits as(
			select 
				coalesce(e.siteid,ldp.siteid) as siteid,coalesce(e.PostID,ldp.id) as postid,
				ldp.date as publish_date,coalesce(sum(hits),0) as hits,e.Event_Action
			from  last_30_before_days_post ldp
			join prod.events e
			on e.postid=ldp.id and e.siteid = ldp.siteid
			and e.date between DATE_SUB(NOW(), INTERVAL 61 DAY) and DATE_SUB(NOW(), INTERVAL  31 DAY) and e.siteid = 14
			and e.Event_Action = "Next Click"
			group by 1,2,3,5
		),
		last_30days_pageview as(
		select 
			coalesce(e.siteid,ldp.siteid) as siteid,
			coalesce(e.PostID,ldp.id) as postid,
			ldp.date as publish_date,coalesce(sum(unique_pageviews),0) as pageviews
		from  last_30days_post ldp
		join prod.pages e
		on e.postid=ldp.id and e.siteid = ldp.siteid
		and e.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) and e.siteid = 14
		group by 1,2,3
		),
		missing_post_in_pages as(
		select * from last_30days_pageview
		union
		select ldp.siteid, ldp.id as postid ,ldp.date as publish_date,0  as pageviews from last_30days_post ldp
		left join last_30days_pageview lp on lp.postid = ldp.id
		where lp.siteid is null
		),
		last_30days_pageview_before as(
			select 
				coalesce(e.siteid,14) as siteid,
				e.PostID,
				ldp.date as publish_date,
				coalesce(sum(unique_pageviews),0) as pageviews 
			from  last_30_before_days_post ldp
			join prod.pages e
			on e.postid=ldp.id and e.siteid = ldp.siteid
			and e.date between DATE_SUB(NOW(), INTERVAL 61 DAY) and DATE_SUB(NOW(), INTERVAL  31 DAY) and e.siteid = 14
			group by 1,2,3
		),
		before_missing_post_in_pages as(
		select * from last_30days_pageview_before
		union
		select ldp.siteid, ldp.id as postid ,ldp.date as publish_date,0  as pageviews from last_30_before_days_post ldp
		left join last_30days_pageview_before lp on lp.postid = ldp.id
		where lp.siteid is null
		),
		last_30days_hits_pages as(
			select 
				l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews,(coalesce(l.hits/p.pageviews,0) * 100) as next_value
			from last_30days_hits l
			left join missing_post_in_pages p on l.postid=p.postId and l.siteid = p.siteid
			where l.siteid = 14
			union 
			select 
				p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews,(coalesce(l.hits/p.pageviews,0) * 100) as next_value
			from last_30days_hits l
			right join missing_post_in_pages p on l.postid=p.postId and l.siteid = p.siteid
			where  p.siteid = 14
		),
		last_30days_hits_pages_before as(
			select 
				l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews,(coalesce(l.hits/p.pageviews,0) * 100) as next_value_before
			from last_30days_before_hits l
			left join before_missing_post_in_pages p on l.postid=p.postId and l.siteid = p.siteid
			where l.siteid = 14
			union 
			select 
				p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews,(coalesce(l.hits/p.pageviews,0) * 100) as next_value_before
			from last_30days_before_hits l
			right join before_missing_post_in_pages p on l.postid=p.postId   and l.siteid = p.siteid
			where  p.siteid = 14
		),
		last_30days_hits_pages_goal as (
			select 
				l.*,
				g.min_pageviews,
				g.Min_CTA,
				case when ((coalesce(coalesce(l.hits,0)/coalesce(l.pageviews,0)*100,0))>=coalesce(g.Min_CTA,0) and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )
		then 1 else 0 end as gt_goal 
			from last_30days_hits_pages l
			join  prod.goals g on g.date=l.publish_date and l.siteid = g.site_id
			where g.Site_ID  =14
		),
		last_30days_hits_pages_goal_before as (
			select
				l.*,
				g.min_pageviews,
				g.Min_CTA,
				case when ((coalesce(coalesce(l.hits,0)/coalesce(l.pageviews,0)*100,0))>=coalesce(g.Min_CTA,0) and coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )
		then 1 else 0 end as gt_goal
			from last_30days_hits_pages_before l
			join  prod.goals g on g.date=l.publish_date and l.siteid = g.site_id
			where g.Site_ID  =14
		),
		last_goal_achived_public_article as(
		select siteid,count(*) as total_publish_article,sum(gt_goal) as  value
		from last_30days_hits_pages_goal
		where siteid = 14
		group by 1
		),
		last_goal_achived_public_article_before as(
		select siteid,count(*) as total_publish_article,sum(gt_goal) as value
		from last_30days_hits_pages_goal_before
		where siteid =14
		group by 1
		)
		select 
		JSON_OBJECT(
				"data", JSON_OBJECT(
					"label",  "Artikler over på begge mål",
					"hint", "Artikler publiceret seneste 30 dage, der både når begge mål ",
					"value", lg.value,
					"change", coalesce(round(((lg.value-pg.value)/pg.value)*100,2),0)
				),
				"site", lg.siteid
			) AS json_output
		from last_goal_achived_public_article lg
		left join last_goal_achived_public_article_before pg on lg.siteid=pg.siteid;

		END


