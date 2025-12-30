{% macro pageview_month_dbt_subscription() %}

{% set siteid = var('site') %}

{% set order_statement_first = var('Traffic').output_referrers_pageviews_channel %}
{% set order_statement_traffic = var('Traffic').output_referrers_pageviews_channel_traffic %}
{% set order_statement_newsletter = var('Traffic').output_referrers_pageviews_channel_newsletter %}

{% set tag_statements = var('Traffic').input_tags_pageviews_channel %}

{% set dropdown_mapping = var('Tendency').dropdown_mapping %}
{% set dropdown_titles = var('Tendency').dropdown_titles %}

{% set count = var('Traffic').dropdown_cte_naming %}
{% set domain = var('Traffic').domain %}

{% set dropdown_val = var('Tendency').dropdown_values %}
{% set dropdown_val_m = var('Tendency').dropdown_values_grouping %}



CREATE PROCEDURE `Traffic_pageview_month_dbt_{{siteid}}`(
    IN site INT,
    IN label_val VARCHAR(200),
    IN hint_val TEXT,
    IN order_statement_first  VARCHAR(2000),
    IN domain VARCHAR(200)
)
BEGIN
    SET SESSION group_concat_max_len = 1000000;

    SET @referrers_sql = CONCAT(
        'SELECT DISTINCT realreferrer FROM pre_stage.ref_value
        WHERE realreferrer IN ( {{order_statement_first}} ) and siteid = {{siteid }}'
    );
    {% for i in range(count | length) %}
        SET @categories_sql{{ count[i] }} = CONCAT(
            'SELECT DISTINCT Categories FROM pre_stage.ref_value 
            WHERE Categories IN ( {{ tag_statements[dropdown_titles[i]] }} ) and siteid = {{ siteid }}'
        );
    {% endfor %}

    SET @sql_query = CONCAT('
        WITH referrers AS (
            ', @referrers_sql, '
            UNION ALL SELECT ''Others''
        ),
        cms_data AS (
            SELECT id, siteid, date, tags, categories, userneeds, link
            FROM prod.site_archive_post
            WHERE siteid = {{siteid}}
        ),
        {% for i in range(count | length) %}

        categories{{ count[i] }} AS (
        ', @categories_sql{{ count[i] }}, '
        UNION ALL SELECT ''Others''
        ),

        combinations{{ count[i] }} AS (
            SELECT r.realreferrer, c.categories AS tag
            FROM referrers r
            CROSS JOIN categories{{ count[i] }} c
            ORDER BY FIELD(r.realreferrer, {{order_statement_first}}, "Others")
        ),
        pageview{{ count[i] }} AS (
            SELECT 
                siteid,
                r.realreferrer,
                postid,
                date AS visit_date,
                COUNT(*) AS visit
            FROM prod.traffic_channels AS t
            LEFT JOIN referrers r ON t.realreferrer = r.realreferrer
            WHERE siteid = {{siteid}} AND date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY)
                AND r.realreferrer IN ( {{ order_statement_traffic }} )
            GROUP BY siteid, r.realreferrer, postid, date
        ),
        main{{ count[i] }} AS (
            SELECT 
                postid,
                siteid,
                event_name,
                SUM(hits) AS t_totals
            FROM prod.events
            WHERE siteid = {{siteid}} AND Event_Action = ''Frontpage'' 
            AND date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY)
            and postid is not null
            GROUP BY postid, siteid, event_name
        ),
        transformed_data{{ count[i] }} AS (
            SELECT
                postid,
                siteid,
                t_totals,
                CASE 
                    WHEN event_name LIKE ''://%'' THEN CONCAT({{ domain }}, SUBSTRING(event_name, 5))
                    ELSE event_name
                END AS modifyurl
            FROM main{{ count[i] }}
        ),
        cms_groups_without_others{{ count[i] }} as(
			{%- for j in range(dropdown_val[dropdown_mapping[dropdown_titles[i]]] | length) %}
            SELECT siteid, id, date, "{{ dropdown_val_m[dropdown_mapping[dropdown_titles[i]]][j] }}" AS tags, link
            FROM cms_data
            WHERE {{ dropdown_mapping[dropdown_titles[i]] }} REGEXP ".*{{dropdown_val[dropdown_mapping[dropdown_titles[i]]][j]}}.*"
            {%- if not loop.last %}
            UNION ALL
            {% endif %}
            {%- endfor %}
        ),
        cms_groups{{ count[i] }} as (
            SELECT a.siteid, a.id, a.date, ''Others'' as tags, a.link
            FROM cms_data a
            LEFT JOIN cms_groups_without_others{{ count[i] }} b
            ON a.siteid = b.siteid AND a.id = b.id
            WHERE b.siteid IS NULL

            UNION ALL

            SELECT * FROM cms_groups_without_others{{ count[i] }}
        ),
        final_transformed{{ count[i] }} AS (
            SELECT
                s.siteid AS siteid,
                p.id AS postid,
                ''Forside'' as realreferrer,
                s.t_totals as visits,
                p.tags
            FROM transformed_data{{ count[i] }} s
            LEFT JOIN cms_groups{{ count[i] }} p ON s.siteid = p.siteid AND s.modifyurl = p.link
            WHERE p.id IS NOT NULL
            and s.siteid = {{siteid}} 
        ),
        site_archive{{ count[i] }} AS (
            SELECT
                s.siteid AS siteid,
                s.id AS postid,
                p.realreferrer,
                p.visit AS visits,
                s.tags
            FROM cms_groups{{ count[i] }} s
            RIGHT JOIN pageview{{count[i]}} p ON s.id = p.postid AND s.siteid = p.siteid
            WHERE s.Siteid = {{siteid}}
        ),
        newsletter_data{{ count[i] }} AS (
            SELECT 
                n.site_id,
                n.referrer_name,
                n.post_id,
                n.date,
                SUM(n.visits) AS visits
                FROM prod.newsletter n
                WHERE n.site_id = {{siteid}} 
                AND n.date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY)
                AND n.referrer_name IN ( {{ order_statement_newsletter }} )
                AND n.post_id IS NOT NULL
                GROUP BY n.site_id, n.referrer_name, n.post_id, n.date
        ),
        grouped_visits{{ count[i] }} AS (
             SELECT
                n.site_id,
                n.post_id,
                n.referrer_name as realreferrer,
                n.visits AS visits,
                s.tags
            FROM newsletter_data{{ count[i] }} n
            LEFT JOIN cms_groups{{ count[i] }} s
                    ON n.post_id = s.id AND s.siteid = n.site_id
        ),
        total_data{{ count[i] }} As(
			select * from site_archive{{ count[i] }}
            UNION
            select * from final_transformed{{ count[i] }}
            UNION
            select * from grouped_visits{{ count[i] }}
        ),
        summed_data{{ count[i] }} AS (
            SELECT
                c.realreferrer AS realreferrer,
                c.tag AS tag,
                COALESCE(SUM(s.visits), 0) AS total_visits
            FROM combinations{{ count[i] }} c
            LEFT JOIN total_data{{ count[i] }} s ON c.realreferrer = s.realreferrer AND c.tag = s.tags
            GROUP BY c.realreferrer, c.tag
			ORDER BY FIELD(c.realreferrer, {{order_statement_first}}, "Others")
        ),
        percent{{ count[i] }} AS (
            SELECT
                realreferrer,
                tag,
                total_visits,
                COALESCE((ROUND((total_visits * 100.0) / COALESCE(SUM(total_visits) OVER (PARTITION BY realreferrer), 0), 2)), 0) AS percentile,
                ROW_NUMBER() OVER (PARTITION BY realreferrer ORDER BY tag) AS tag_order
            FROM summed_data{{ count[i] }}
        ),
        pivoted_data{{ count[i] }} AS (
			SELECT
				realreferrer,
				CONCAT(''['', GROUP_CONCAT(DISTINCT CONCAT(''"'', tag, ''"'') ORDER BY FIELD(tag, {{tag_statements[dropdown_titles[i]]}}, "Others")), '']'') AS categories,
				GROUP_CONCAT(total_visits ORDER BY FIELD(tag, {{tag_statements[dropdown_titles[i]]}}, "Others")) AS total_visits,
				CONCAT(''['', GROUP_CONCAT((percentile) ORDER BY FIELD(tag, {{tag_statements[dropdown_titles[i]]}}, "Others")), '']'') AS percentile
			FROM percent{{ count[i] }}
			GROUP BY realreferrer
             ORDER BY FIELD(realreferrer, {{order_statement_first}}, "Others")
		),
        final{{count[i]}} as(
			SELECT 	
				''Hvilken type indhold skaber engagement på tværs af trafikkanaler?'' AS label,
                ''Andelen af trafik til indgangssider for de forskellige trafikkanaler grupperet ift. indgangssidernes tags/kategorier. For Forside er artikler, der klikkes på fra forsiden i stedet for indgangssider.'' AS hint,
				categories AS cat, 
				CONCAT(''['', GROUP_CONCAT(''{"name": "'', realreferrer, ''","data":'', percentile, ''}''), '']'') AS series
			FROM pivoted_data{{ count[i] }}
			group by label,cat,hint
		)
        {%- if not loop.last %}, {% endif %}
        {% endfor %}

		SELECT 
			CONCAT(
				''{'',
					''"defaultTitle": "{{dropdown_titles[0]}}",'',
					''"label": "'', jd.label, ''",'',
					''"hint": "'', jd.hint, ''",'',
					''"categories": '', jd.cat, '','',
					''"series": '', jd.series, '','',
					''"additional": ['',
                    {% for i in range(1, count | length) %}
						''{'',
							''"title": "{{dropdown_titles[i]}}",'',
							''"data": {'',
								''"label": "'', final{{count[i]}}.label, ''",'',
								''"categories": '', final{{count[i]}}.cat, '','',
								''"series": '', final{{count[i]}}.series, ''''
							''}''
						''}'' 
                    {% if not loop.last %}'',''{% endif %}
                    {% endfor %}
					'']''
				''}''
			) AS json_data
		FROM final{{count[0]}} jd
        {% for i in range(1, count | length) %}
        CROSS JOIN final{{ count[i] }}
        {% endfor %};
    ');

    PREPARE dynamic_query FROM @sql_query;
    EXECUTE dynamic_query;
    DEALLOCATE PREPARE dynamic_query;
END
{% endmacro %}