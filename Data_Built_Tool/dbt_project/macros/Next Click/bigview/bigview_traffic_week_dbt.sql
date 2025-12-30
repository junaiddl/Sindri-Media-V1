{% macro bigview_traffic_week_dbt() %}

{% set order_statements = var('Traffic').input_referrers_linechart %}
{% set update_order_statement = var('Traffic').output_referrers_linechart %}
{% set siteid = var('site') %}

CREATE PROCEDURE `bigview_traffic_week_dbt_{{ siteid }}`()
BEGIN
   SET SESSION group_concat_max_len = 100000;
    
    SET @referrers_sql = CONCAT(
        'SELECT DISTINCT realreferrer FROM pre_stage.ref_value WHERE realreferrer IN ( {{ order_statements }} ) and siteid = {{ siteid }}'
    );
    
    SET @sql_query = CONCAT('
        WITH referrers AS (
            ', @referrers_sql, '
            UNION ALL SELECT ''Others''
        ),
        distinct_dates AS (
			SELECT DISTINCT date
			FROM prod.goals
			WHERE date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
			AND site_id = {{ siteid }}
			ORDER BY date
		),
		cross_joined AS (
			SELECT date, realreferrer
			FROM distinct_dates
			CROSS JOIN referrers
		),
		agg_on_day AS (
			SELECT
				d.date,
				t.siteid,
				CASE
					WHEN t.realreferrer IN (SELECT realreferrer FROM referrers) THEN t.realreferrer
					WHEN t.realreferrer LIKE ''NL%'' THEN "Newsletter"
					ELSE ''Others''
				END AS realreferrer
			FROM distinct_dates d
			LEFT JOIN prod.traffic_channels t ON d.date = t.date AND t.siteid = {{ siteid }}
			AND t.date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
		),
		summed_data AS (
			SELECT
				c.date,
				c.realreferrer,
				COALESCE(COUNT(a.date), 0) AS visit
			FROM cross_joined c
			LEFT JOIN agg_on_day a ON c.date = a.date AND c.realreferrer = a.realreferrer
			GROUP BY c.date, c.realreferrer
		),
		count_cte AS (
			SELECT
				t.date AS cte_date,
				t.realreferrer,
				COALESCE(visit, 0) AS referrer_count
			FROM summed_data t
			LEFT JOIN distinct_dates d ON t.date = d.date
		),
		real__format AS (
			SELECT cte_date AS date,
				case 
                WHEN realreferrer like ''NL%'' THEN ''Newsletter''
                else realreferrer
				END AS realreferrer,
				referrer_count AS referrer_count
			FROM count_cte
		),
		real_format as(
			SELECT date,realreferrer,sum(referrer_count) AS referrer_count
			FROM real__format group by date, realreferrer
		),
		sequence AS (
			SELECT
				date,
				realreferrer,
				coalesce(referrer_count,0) as referrer_count
			FROM real_format
		),
		json_data as (
			SELECT
				realreferrer,
				GROUP_CONCAT(CONCAT(''"'', DATE_FORMAT(date, ''%d.%m''), ''"'') ORDER BY date SEPARATOR '', '') AS categories,
				CONCAT(''['', GROUP_CONCAT(referrer_count ORDER BY date SEPARATOR '', ''), '']'') AS referrer_counts
			FROM sequence
			GROUP BY realreferrer
		),
		final_ar as (
			SELECT
            {{ siteid }} as siteid ,
			''Besøg'' as label, ''Besøg opdelt på trafikkanaler'' as hint,
			  categories AS categories,
				GROUP_CONCAT(realreferrer ORDER BY FIELD(LOWER(realreferrer),  {{update_order_statement}} , ''others'' ) SEPARATOR '','') AS series,
			  GROUP_CONCAT(referrer_counts ORDER BY FIELD(LOWER(realreferrer),  {{update_order_statement}} , ''others'') SEPARATOR '','') AS data
			FROM json_data
			GROUP BY categories
		)
        SELECT
				CONCAT( 
                ''{'',
					''"site": {{siteid}},'', 
					''"data":	{
                    "categories":'' ,''['', categories, ''],'',
					''"series":'',''['', CONCAT(''"'', REPLACE(series, '','', ''","''), ''"''), ''],'',
					''"data":'',''['', data, '']
                    }}''
				) AS json_data
			FROM
				final_ar;
    ');

    PREPARE dynamic_query FROM @sql_query;
    EXECUTE dynamic_query;
    DEALLOCATE PREPARE dynamic_query;
END
{% endmacro %}