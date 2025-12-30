


    





CREATE PROCEDURE `Traffic_dynamic_linequery_month_dbt_17`(
    IN site INT,
    IN label_val VARCHAR(200),
    IN hint_val TEXT,
    IN order_statement VARCHAR(2000),
	IN update_order_statement VARCHAR(2000)
)
BEGIN
    SET SESSION group_concat_max_len = 100000;

    SET @referrers_sql = CONCAT(
        'SELECT DISTINCT realreferrer FROM pre_stage.ref_value WHERE realreferrer IN ( ''Direct'', ''Search'', ''Facebook'', ''Newsletter'' ) and siteid = 17 '
    );

    SET @sql_query = CONCAT('
        WITH referrers AS (
            ', @referrers_sql, '
            UNION ALL SELECT ''Others''
        ),
		distinct_months as(
			SELECT DISTINCT 
				DATE_FORMAT(date, ''%Y-%m'') AS month_name
			FROM prod.goals
			WHERE date BETWEEN DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 YEAR), INTERVAL 1 MONTH) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
				AND site_id = 17
			ORDER BY month_name
		),
		cross_joined AS (
			SELECT month_name, realreferrer
			FROM distinct_months
			CROSS JOIN referrers
		),
		agg_on_day AS (
			SELECT
				d.month_name,
                t.date,
				t.siteid,
				CASE
					WHEN t.realreferrer IN (SELECT realreferrer FROM referrers) THEN t.realreferrer
					WHEN t.realreferrer LIKE ''NL%'' THEN "Newsletter"
					ELSE ''Others''
				END AS realreferrer
			FROM distinct_months d
			LEFT JOIN prod.traffic_channels t ON d.month_name = DATE_FORMAT(t.date, ''%Y-%m'') AND t.siteid = 17
			AND t.date between DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 YEAR), INTERVAL 1 MONTH) and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
		),
		summed_data as(
			SELECT
				c.month_name as modified_date,
				c.realreferrer,
				COALESCE(COUNT(a.month_name), 0) AS visit
			FROM cross_joined c
			LEFT JOIN agg_on_day a ON c.month_name = a.month_name AND c.realreferrer = a.realreferrer
			GROUP BY c.month_name, c.realreferrer
		),
		count_cte AS (
			SELECT
				t.modified_date,
				t.realreferrer,
				COALESCE(visit, 0) AS referrer_count
			FROM summed_data t
			LEFT JOIN distinct_months d ON t.modified_date = d.month_name
		),
        real__format AS (
			SELECT modified_date,
				CASE 
					WHEN realreferrer like ''NL%'' THEN ''Newsletter''
					ELSE realreferrer
				END AS realreferrer,
				referrer_count
			FROM count_cte
        ),
		real_format as(
			SELECT modified_date,realreferrer,sum(referrer_count) AS referrer_count
			FROM real__format 
            group by modified_date,realreferrer 
		),
		json_data AS (
			SELECT
				realreferrer,
				GROUP_CONCAT(CONCAT(''"'', modified_date, ''"'') ORDER BY modified_date  SEPARATOR '', '') AS categories,
				CONCAT(''['', GROUP_CONCAT(referrer_count ORDER BY modified_date SEPARATOR '', ''), '']'') AS referrer_counts
			FROM real_format
			GROUP BY realreferrer
		),
        final_ar AS (
            SELECT
                17 AS siteid,
                ''Besøg'' AS label, ''Besøg opdelt på trafikkanaler'' AS hint,
                categories AS categories,
                GROUP_CONCAT(realreferrer ORDER BY FIELD(LOWER(realreferrer), ''Direct'', ''Search'', ''Facebook'', ''Newsletter'' , ''others'' ) SEPARATOR '','') AS series,
                GROUP_CONCAT(referrer_counts ORDER BY FIELD(LOWER(realreferrer), ''Direct'', ''Search'', ''Facebook'', ''Newsletter'' , ''others'') SEPARATOR '','') AS data
            FROM json_data
            GROUP BY categories
        )
        SELECT
            CONCAT( 
                ''{'',
                    ''"site": 17,'', 
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

