{% macro dynamic_linequery_month_dbt_sales() %}

{% set order_statements = var('Traffic').input_referrers_linechart %}
{% set update_order_statement = var('Traffic').output_referrers_linechart %}
{% set siteid = var('site') %}

CREATE PROCEDURE `Traffic_dynamic_linequery_month_dbt_{{ siteid }}`(
    IN site INT,
    IN label_val VARCHAR(200),
    IN hint_val TEXT,
    IN order_statement VARCHAR(2000),
	IN update_order_statement VARCHAR(2000)
)
BEGIN
    SET SESSION group_concat_max_len = 100000;

    SET @referrers_sql = CONCAT(
        'SELECT DISTINCT realreferrer FROM pre_stage.ref_value WHERE realreferrer IN ( {{ order_statements }} ) and siteid = {{siteid}} '
    );

    SET @sql_query = CONCAT('
        WITH referrers AS (
            ', @referrers_sql, '
            UNION ALL SELECT ''Others''
        ),
        distinct_months as
		(
			SELECT DISTINCT 
				DATE_FORMAT(date, ''%M'') AS month_name
			FROM prod.traffic_channels
			WHERE date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
				AND siteid = {{ siteid }}
			ORDER BY month_name
		),
		cross_joined AS (
			SELECT month_name, realreferrer
			FROM distinct_months
			CROSS JOIN referrers
		),
		agg_on_day AS (
			SELECT
				month_name,
				t.siteid,
				CASE
					WHEN t.realreferrer IN (SELECT realreferrer FROM referrers) THEN t.realreferrer
					WHEN t.realreferrer LIKE ''NL%'' THEN "Newsletter"
					ELSE ''Others''
				END AS realreferrer
			FROM distinct_months d
			LEFT JOIN prod.traffic_channels t ON d.month_name = DATE_FORMAT(t.date, ''%M'') AND t.siteid = {{ siteid }}
			AND t.date between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
		),
		summed_data as
		(
			SELECT
				c.month_name,
				c.realreferrer,
				COALESCE(COUNT(a.month_name), 0) AS visit
			FROM cross_joined c
			LEFT JOIN agg_on_day a ON c.month_name = a.month_name AND c.realreferrer = a.realreferrer
			GROUP BY c.month_name, c.realreferrer
		),
		count_cte AS (
			SELECT
				t.month_name AS month_name,
				t.realreferrer,
				COALESCE(visit, 0) AS referrer_count
			FROM summed_data t
			LEFT JOIN distinct_months d ON t.month_name = d.month_name
		)
		,
		real__format AS (
			SELECT month_name AS month_name,
				case 
                WHEN realreferrer like ''NL%'' THEN ''Newsletter''
                else realreferrer
				END AS realreferrer,
				referrer_count AS referrer_count
			FROM count_cte),
		real_format as(
        SELECT month_name,realreferrer,sum(referrer_count) AS referrer_count
			FROM real__format group by month_name,realreferrer )
		,
		sequence AS (
			SELECT
				month_name,
				realreferrer,
				coalesce(referrer_count,0) as referrer_count
			FROM real_format
		),
		json_data AS (
			SELECT
				realreferrer,
				GROUP_CONCAT(CONCAT(''"'', month_name, ''"'') ORDER BY FIELD(month_name, ''January'', ''February'', ''March'', ''April'', ''May'', ''June'', ''July'', ''August'', ''September'', ''October'', ''November'', ''December'') SEPARATOR '', '') AS categories,
				CONCAT(''['', GROUP_CONCAT(referrer_count ORDER BY FIELD(month_name, ''January'', ''February'', ''March'', ''April'', ''May'', ''June'', ''July'', ''August'', ''September'', ''October'', ''November'', ''December'') SEPARATOR '', ''), '']'') AS referrer_counts
			FROM sequence
			GROUP BY realreferrer
		),

        final_ar AS (
            SELECT
                {{ siteid }} AS siteid,
                ''Besøg'' AS label, ''Besøg opdelt på trafikkanaler'' AS hint,
                categories AS categories,
                GROUP_CONCAT(realreferrer ORDER BY FIELD(LOWER(realreferrer), {{update_order_statement}} , ''others'' ) SEPARATOR '','') AS series,
                GROUP_CONCAT(referrer_counts ORDER BY FIELD(LOWER(realreferrer), {{update_order_statement}} , ''others'') SEPARATOR '','') AS data
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