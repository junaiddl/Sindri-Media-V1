{% macro string_tactical_userneeds_pageviews_chart_month_dbt_nextclick() %}

{% set count = var('Tendency').dropdown_cte_naming %}
{% set dropdown_titles = var('Tendency').dropdown_titles %}
{% set dropdown_count = var('Tendency').dropdown_mapping %}
{% set dropdown_val = var('Tendency').dropdown_values %}
{% set dropdown_val_m = var('Tendency').dropdown_values_grouping %}
{% set order_statements = var('Tendency').order_statements %}
{% set tag_filters_exclude = var('condition') %}



{% set site = var('site') %}
{% set event_name = var('event_action') %}

CREATE PROCEDURE `tactical_userneeds_pageviews_chart_month_dbt_{{site}}`()
BEGIN
	with cms_data as (
            SELECT siteid, id, date as fdate, Categories, userneeds, tags
            FROM prod.site_archive_post_string
            WHERE
            date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY)
            AND DATE_SUB(NOW(), INTERVAL 1 DAY)
            AND siteid = {{site}}
            {% if tag_filters_exclude.status %}
                {{tag_filters_exclude.query}}
            {% endif %}
        ),
    {% for i in range(count | length) %}
		last_30_days_article_published_without_others{{ count[i] }} as(
			{%- for j in range(dropdown_val[dropdown_count[dropdown_titles[i]]] | length) %}
                SELECT siteid, id, fdate, "{{ dropdown_val_m[dropdown_count[dropdown_titles[i]]][j] }}" AS tags
                FROM cms_data
                WHERE {{ dropdown_count[dropdown_titles[i]] }} REGEXP ".*{{dropdown_val[dropdown_count[dropdown_titles[i]]][j]}}.*"
                {%- if not loop.last %}
                UNION ALL
                {% endif %}
                {%- endfor %} 
        ),
		last_30_days_article_published{{ count[i] }} as (
                SELECT a.siteid, a.id, a.fdate, 'others' as Tags
                FROM cms_data a
                LEFT JOIN last_30_days_article_published_without_others{{ count[i] }} b
                ON a.siteid = b.siteid AND a.id = b.id
                WHERE b.siteid IS NULL

                UNION ALL

                SELECT * FROM last_30_days_article_published_without_others{{ count[i] }}
            ),
	 last_30_days_article_published_Agg{{ count[i] }} as(
		select 
			siteid as siteid,
            tags,
            count(*) as agg 
		from  last_30_days_article_published{{ count[i] }}
		where siteid = {{site}}
		group by 1,2
		),
	  agg_next_click_per_article{{ count[i] }} as 
      (
		select  
			e.siteid as siteid,
            e.id as postid,
            e.tags,
            coalesce(sum(hits),0) as val
		from last_30_days_article_published{{ count[i] }}  e
		left join prod.events_string a on a.postid=e.id and a.siteid = e.siteid and a.event_action = '{{event_name}}'
		where    e.siteid = {{site}}
		group by 1,2,3
		),
	agg_pages_per_article{{ count[i] }} as 
      (
		select  
			e.siteid as siteid,
            e.id as postid,
            e.tags,
			coalesce(sum(unique_pageviews),0) as page_view 
		from last_30_days_article_published{{ count[i] }}  e
		left join prod.pages_string p on p.postid=e.id and p.siteid =e.siteid
		where    e.siteid = {{site}}
		group by 1,2,3
		)  ,
	agg_total{{ count[i] }} as (
			select
			e.siteid as siteid,
			e.id as postid,
			e.tags as tags,
			p.page_view as page_view,
			a.val as val
			from last_30_days_article_published{{ count[i] }}  e
			left join agg_pages_per_article{{ count[i] }} p on p.postid=e.id and p.siteid =e.siteid
			left join agg_next_click_per_article{{ count[i] }}  a on a.postid=e.id and a.siteid = e.siteid
        ) ,
		cta_per_article{{ count[i] }} as
		( 
			select  e.siteid as siteid, e.postid, e.tags,
			round(coalesce((coalesce(sum(val),0)/coalesce(sum(page_view),1)),0.0)*100.0,2) as val from agg_total{{ count[i] }}  e
			where  e.siteid = {{site}}
			group by 1,2,3

		),
	agg_cta_per_article{{ count[i] }} as(
		select siteid as siteid,tags,sum(val) as agg_sum from  cta_per_article{{ count[i] }}
		where siteid = {{site}}
		group by 1,2
	),
	less_tg_data{{ count[i] }} as(
		select 
		postid,l.tags ,val,min_cta
		,a.siteid,
		case when val<min_cta then 1 else 0 end as less_than_target
		,percent_rank() over ( order BY case when val>=min_cta then val-min_cta else 0 end) as percentile
		 from last_30_days_article_published{{ count[i] }} l
		join cta_per_article{{ count[i] }} a on l.siteid = a.siteid and l.id = a.postid and l.tags=a.tags
		join prod.goals g on a.siteid = g.site_id and g.Date = l.fdate
		 where date  between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) 
         and g.site_id = {{site}}
		),
	counts{{ count[i] }} as (
			select 
				siteid,
				tags,
				sum(less_than_target) as less_than_target_count,
				sum(case when percentile <= 1 and percentile >= 0.9 then 1 else 0 end) as top_10_count,
				sum(case when percentile >= 0 and percentile < 0.9 and less_than_target = 0 then 1 else 0 end) as approved_count
			from less_tg_data{{ count[i] }}
			group by 1,2
		)
		,
		hits_by_tags{{ count[i] }} as(
			select 
				siteid as siteid,
				tags,
				sum(less_than_target) as hits_tags,
				sum(case when percentile<=1 and percentile>=0.9 then val else 0 end) sum_by_top_10,
				sum(case when percentile>=0 and percentile<0.9 and less_than_target=0 then val else 0 end) sum_by_approved
		   from less_tg_data{{ count[i] }}
		   where siteid = {{site}}
		   group by 1,2
		),

		total_hits{{ count[i] }} as(
			select 
				siteid as siteid ,
				sum(less_than_target_count) as total_tags_hit,
                sum(top_10_count) as agg_by_top_10,
                sum(approved_count) as agg_by_approved 
			from counts{{ count[i] }}
			where  siteid = {{site}}
			group by 1
		),
		agg_data{{ count[i] }} as(
			select 
				h.siteid as siteid ,
				h.tags,
				coalesce(less_than_target_count/total_tags_hit,0)*100 as less_than_target ,
				coalesce(top_10_count/agg_by_top_10,0)*100 as top_10,
				coalesce(approved_count/agg_by_approved,0)*100 as  approved
			from counts{{ count[i] }} h
			join total_hits{{ count[i] }} t on h.siteid=t.siteid
			join last_30_days_article_published_Agg{{ count[i] }} hbt on hbt.siteid=h.siteid and hbt.tags=h.tags
			where h.siteid  = {{site}}
		)
		,
		categories_d{{ count[i] }} as (
			select 
				siteid as siteid ,
				'Artikler ift. next click mål' as label,
                'Artikler grupperet ift hvordan de har performet på next click' as hint,
				GROUP_CONCAT(tags ORDER BY FIELD(tags, {{ order_statements[dropdown_count[dropdown_titles[i]]] }}, 'others' ) SEPARATOR ',') AS cateogires,
				GROUP_CONCAT(COALESCE(less_than_target, 0) ORDER BY FIELD(tags, {{ order_statements[dropdown_count[dropdown_titles[i]]] }}, 'others' ) SEPARATOR ',') AS less_than_target,
			GROUP_CONCAT(approved ORDER BY FIELD(tags, {{ order_statements[dropdown_count[dropdown_titles[i]]] }}, 'others' ) SEPARATOR ',') AS approved,
			GROUP_CONCAT(top_10 ORDER BY FIELD(tags, {{ order_statements[dropdown_count[dropdown_titles[i]]] }}, 'others' ) SEPARATOR ',') AS top_10
		from agg_data{{ count[i] }}
		group by 1,2,3
		),
        categories{{ count[i] }} as (
		select siteid as siteid ,label as label, hint as hint,  
		 CONCAT('"', REPLACE(cateogires, ',', '","'), '"') AS  cateogires,
		less_than_target as less_than_target ,
		approved as approved ,
		 top_10 as top_10 
		 from  categories_d{{ count[i] }}
		),
		json_data{{ count[i] }} as (
		select siteid,label as lab,hint as h,cateogires as cat,CONCAT(
		'{"name": "Under mål", "data": [',cast(less_than_target as char),']}'
		,',{"name": "Over mål" ,"data": [',cast(approved as char),']}',',
        {"name": "Top 10%" ,"data": [',cast(top_10 as char),']}') 
        as series from categories{{ count[i] }} 
		)
    {%- if not loop.last %}, {% endif %}
    {% endfor %}

    SELECT 
        CONCAT(
            '{',
                '"site":', jd.siteid, ',',
                '"data": {',
                    '"label": "', jd.lab, '",',
                    '"hint": "', jd.h, '",',
                    '"categories": [', jd.cat, '],',
                    '"series": [', jd.series, '],',
                    '"defaultTitle": "{{dropdown_titles[0]}}",',
                    '"additional": [',
                    {% for i in range(1, count | length) %}
                    '{',
                        '"title": "{{dropdown_titles[i]}}",',
                        '"data": {',
                            '"label": "', json_data{{ count[i] }}.lab, '",',
                            '"categories": [', json_data{{ count[i] }}.cat, '],',
                            '"series": [', json_data{{ count[i] }}.series, ']',
                        '}',
                    '}'
                    {% if not loop.last %}','{% endif %}
                    {% endfor %}
                    ']',
                '}',
            '}'
        ) AS json_data
    FROM json_data jd
    {% for i in range(1, count | length) %}
    CROSS JOIN json_data{{ count[i] }}
    {% endfor %};
END

{% endmacro %}