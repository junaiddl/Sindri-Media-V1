{% macro dynamic_linequery_week_dbt_sales() %}

{% set order_statements = var('Traffic').input_referrers_linechart %}
{% set update_order_statement = var('Traffic').output_referrers_linechart %}
{% set siteid = var('site') %}

CREATE PROCEDURE `Traffic_dynamic_linequery_week_dbt_{{ siteid }}`(
    IN site INT,
    IN label_val VARCHAR(200),
    IN hint_val TEXT,
    IN order_statement VARCHAR(2000),
	IN update_order_statement VARCHAR(2000)
)
BEGIN
    SET SESSION group_concat_max_len = 100000;

    SET @referrers_sql = CONCAT(
        'SELECT DISTINCT realreferrer FROM pre_stage.ref_value WHERE realreferrer IN ( {{ order_statements }} ) and siteid = {{ siteid}}'
    );

    SET @sql_query = CONCAT('
        WITH referrers AS (
            ', @referrers_sql, '
            UNION ALL SELECT ''Others''
        ),
        distinct_weeks as
		(
			SELECT DISTINCT 
				WEEK(date,1) AS week_number
			FROM prod.traffic_channels
			WHERE date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
				AND siteid = {{ siteid }}
			ORDER BY week_number
		),
		cross_joined AS (
			SELECT week_number, realreferrer
			FROM distinct_weeks
			CROSS JOIN referrers
		),
		agg_on_day AS (
			SELECT
				week_number,
				t.siteid,
				CASE
					WHEN t.realreferrer IN (SELECT realreferrer FROM referrers) THEN t.realreferrer
					WHEN t.realreferrer LIKE ''NL%'' THEN "Newsletter"
					ELSE ''Others''
				END AS realreferrer
			FROM distinct_weeks d
			LEFT JOIN prod.traffic_channels t ON d.week_number = WEEK(t.date,1) AND t.siteid = {{ siteid }}
			AND t.date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
		),
		summed_data as
		(
			SELECT
				c.week_number,
				c.realreferrer,
				COALESCE(COUNT(a.week_number), 0) AS visit
			FROM cross_joined c
			LEFT JOIN agg_on_day a ON c.week_number = a.week_number AND c.realreferrer = a.realreferrer
			GROUP BY c.week_number, c.realreferrer
		),
		count_cte AS (
			SELECT
				t.week_number AS cte_week,
				t.realreferrer,
				COALESCE(visit, 0) AS referrer_count
			FROM summed_data t
			LEFT JOIN distinct_weeks d ON t.week_number = d.week_number
		)
		,
		real__format AS (
			SELECT cte_week AS week_number,
				case 
                WHEN realreferrer like ''NL%'' THEN ''Newsletter''
                else realreferrer
				END AS realreferrer,
				referrer_count AS referrer_count
			FROM count_cte),
		real_format as(
        SELECT week_number,realreferrer,sum(referrer_count) AS referrer_count
			FROM real__format group by week_number,realreferrer )
		,
		sequence AS (
			SELECT
				week_number,
				realreferrer,
				coalesce(referrer_count,0) as referrer_count
			FROM real_format
		),
        json_data AS (
            SELECT
                realreferrer,
                GROUP_CONCAT(CONCAT(''"'', week_number, ''"'') ORDER BY week_number SEPARATOR '', '') AS categories,
                CONCAT(''['', GROUP_CONCAT(referrer_count ORDER BY week_number SEPARATOR '', ''), '']'') AS referrer_counts
            FROM sequence
            GROUP BY realreferrer
        ),
        final_ar AS (
            SELECT
                {{ siteid }} AS siteid,
                ''Besøg'' AS label, ''Besøg opdelt på trafikkanaler'' AS hint,
                categories AS categories,
                GROUP_CONCAT(realreferrer ORDER BY FIELD(LOWER(realreferrer),  {{update_order_statement}} , ''others'' ) SEPARATOR '','') AS series,
                GROUP_CONCAT(referrer_counts ORDER BY FIELD(LOWER(realreferrer),   {{update_order_statement}} , ''others'') SEPARATOR '','') AS data
            FROM json_data
            GROUP BY categories
        )
        SELECT
            CONCAT( 
                ''{'',
                    ''"site": {{ siteid }},'', 
                    ''"data":	{"label":"'', label,
                    ''","hint":"'', hint,
                    ''","categories":'' ,''['', categories, ''],'',
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