{% macro string_next_click_performance_articles1_card_month_dbt() %}		
		
		{% set site = var('site') %}
		{% set event = var('event_action') %}
		{% set status = var('condition')['status'] %}
		{% set query = var('condition')['query'] %}


		CREATE PROCEDURE `performance_articles1_card_month_dbt_{{site}}`()
		BEGIN

		WITH last_30days_post AS (
			SELECT 
				siteid as siteid,
				ID,
				date as Date
			FROM prod.site_archive_post_string
			where date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY)
			{% if status %}
			{{query}}
			{% endif %}
			and siteid = {{site}}
		),
		last_30_before_days_post AS (
			SELECT 
				siteid as siteid,
				ID,
				date as Date
			FROM prod.site_archive_post_string
			where date  between DATE_SUB(NOW(), INTERVAL 61 DAY ) and DATE_SUB(NOW(), INTERVAL  31 DAY)
			{% if status %}
			{{query}}
			{% endif %}
			and siteid = {{site}}
		),
		last_30days_hits as(
			select 
				coalesce(e.siteid,{{site}}) as siteid,
				coalesce(e.PostID,ldp.ID) as postid,
				ldp.date as publish_date,
				coalesce(sum(hits),0) as hits
			from  last_30days_post ldp
			left join prod.events_string e
			on e.postid=ldp.id and ldp.siteid=e.siteid
			and e.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) and e.siteid = {{site}}
			and e.Event_Action = "{{event}}"
			group by 1,2,3
		),
		last_30days_before_hits as(
			select 
				coalesce(e.siteid,{{site}}) as siteid,
				coalesce(e.PostID,ldp.ID) as postid,
				ldp.date as publish_date,
				coalesce(sum(hits),0) as hits
			from  last_30_before_days_post ldp
			left join prod.events_string e
			on e.postid=ldp.id and ldp.siteid=e.siteid
			and e.date between DATE_SUB(NOW(), INTERVAL 61 DAY) and DATE_SUB(NOW(), INTERVAL  31 DAY) and e.siteid = {{site}}
			and e.Event_Action = "{{event}}"
			group by 1,2,3
		),
		last_30days_pageview as(
			select 
				coalesce(e.siteid,{{site}}) as siteid,
				coalesce(e.PostID,ldp.ID) as postid,
				ldp.date as publish_date,
				coalesce(sum(unique_pageviews),0) as pageviews 
			from  last_30days_post ldp
			join prod.pages_string e
			on e.postid=ldp.id and e.siteid = ldp.siteid
			and e.date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL  1 DAY) and e.siteid = {{site}}
			group by 1,2,3
		),
		last_30days_pageview_before as(
			select 
				coalesce(e.siteid,{{site}}) as siteid,
				coalesce(e.PostID,ldp.Id) as postid,
				ldp.date as publish_date,
				coalesce(sum(unique_pageviews),0) as pageviews 
			from  last_30_before_days_post ldp
			join prod.pages_string e
			on e.postid=ldp.id and e.siteid = ldp.siteid
			where e.date between DATE_SUB(NOW(), INTERVAL 61 DAY) and DATE_SUB(NOW(), INTERVAL  31 DAY) and e.siteid = {{site}}
			group by 1,2,3
		),
		last_30days_hits_pages as(
			select 
				l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews,(l.hits/p.pageviews * 100) as next_value
			from last_30days_hits l
			left join last_30days_pageview p on l.postid=p.postId and l.siteid = p.siteid
			where l.siteid = {{site}} 
			union 
			select 
				p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews,(l.hits/p.pageviews * 100) as next_value
			from last_30days_hits l
			right join last_30days_pageview p on l.postid=p.postId and l.siteid = p.siteid
			where  p.siteid = {{site}} 
		),
		last_30days_hits_pages_before as(
		select 
			l.siteid as siteid,l.postid,l.publish_date,l.hits,p.pageviews,(coalesce(l.hits/p.pageviews,0) * 100) as next_value_before
		from last_30days_before_hits l
		left join last_30days_pageview_before p on l.postid=p.postId and l.siteid = p.siteid
		where l.siteid = {{site}} 
		union 
		select 
			p.siteid as siteid,p.postid,p.publish_date,l.hits,p.pageviews,(coalesce(l.hits/p.pageviews,0) * 100) as next_value_before
		from last_30days_before_hits l
		right join last_30days_pageview_before p on l.postid=p.postId   and l.siteid = p.siteid
		where  p.siteid = {{site}} 
		),
		last_30days_hits_pages_goal as (
			select 
				l.*,
				g.min_pageviews,
				g.Min_CTA,
				case 
					when 
						(coalesce(next_value,0)>=coalesce(g.Min_CTA,0)
						or 
						coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )
					and 
						!(coalesce(next_value,0)>=coalesce(g.Min_CTA,0)
						and 
						coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) ) 
					then 1 else 0 end as gt_goal  
			from last_30days_hits_pages l
			join  prod.goals g on g.date=l.publish_date and l.siteid = g.site_id
			where g.Site_ID = {{site}}
		),
		last_30days_hits_pages_goal_before as (
			select 
				l.*,
				g.min_pageviews,
				g.Min_CTA,
				case 
					when 
						(coalesce(next_value_before,0)>=coalesce(g.Min_CTA,0) 
						or 
						coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )
					and 
						!(coalesce(next_value_before,0)>=coalesce(g.Min_CTA,0) 
						and 
						coalesce(l.pageviews,0)>=coalesce(g.Min_pageviews,0) )
					then 1 else 0 end as gt_goal 
			from last_30days_hits_pages_before l
			join  prod.goals g on g.date=l.publish_date and l.siteid = g.site_id
			where g.Site_ID  ={{site}}
		),
		last_goal_achived_public_article as(
		select siteid,count(*) as total_publish_article,sum(gt_goal) as  value
		from last_30days_hits_pages_goal
		where siteid = {{site}}
		group by 1
		),
		last_goal_achived_public_article_before as(
		select siteid,count(*) as total_publish_article,sum(gt_goal) as value
		from last_30days_hits_pages_goal_before
		where siteid ={{site}}
		group by 1
		)
		select 
		JSON_OBJECT(
				"data", JSON_OBJECT(
					"label", "Artikler over på ét af målene",
					"hint", "Artikler publiceret seneste 30 dage, der når ét af målene ",
					"value", coalesce(lg.value,0),
					"change", coalesce(round(((lg.value-pg.value)/pg.value)*100,2),0)
				),
				"site", coalesce(lg.siteId,pg.siteid)
			) AS json_output
		from last_goal_achived_public_article lg
		left join last_goal_achived_public_article_before pg on lg.siteid=pg.siteid;

		END

{% endmacro %}