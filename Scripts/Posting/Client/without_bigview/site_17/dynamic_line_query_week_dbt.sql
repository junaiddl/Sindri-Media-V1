


    





CREATE PROCEDURE `Traffic_dynamic_linequery_week_dbt_17`(
    IN site INT,
    IN label_val VARCHAR(200),
    IN hint_val TEXT,
    IN order_statement VARCHAR(2000),
	IN update_order_statement VARCHAR(2000)
)
BEGIN
    SET SESSION group_concat_max_len = 100000;

    SET @referrers_sql = CONCAT(
        'SELECT DISTINCT realreferrer FROM pre_stage.ref_value WHERE realreferrer IN ( ''Direct'', ''Search'', ''Facebook'', ''Newsletter'' ) and siteid = 17'
    );

    SET @sql_query = CONCAT('
        WITH referrers AS (
            ', @referrers_sql, '
            UNION ALL SELECT ''Others''
        ),
       	distinct_weeks as (
			SELECT DISTINCT 
				DATE_FORMAT(date, ''%Y-%u'') AS year_week
			FROM prod.goals
			WHERE date BETWEEN DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 YEAR), INTERVAL 1 MONTH) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
				AND site_id = 17
			ORDER BY year_week
		),
		cross_joined AS (
			SELECT year_week, realreferrer
			FROM distinct_weeks
			CROSS JOIN referrers
		),
		agg_on_day AS (
			SELECT
				year_week,
                t.date,
				t.siteid,
				CASE
					WHEN t.realreferrer IN (SELECT realreferrer FROM referrers) THEN t.realreferrer
					WHEN t.realreferrer LIKE ''NL%'' THEN "Newsletter"
					ELSE ''Others''
				END AS realreferrer
			FROM distinct_weeks d
			LEFT JOIN prod.traffic_channels t ON d.year_week = DATE_FORMAT(t.date, ''%Y-%u'') AND t.siteid = 17
			AND t.date between DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 YEAR), INTERVAL 1 MONTH)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
		),
		summed_data as (
			SELECT
				c.year_week,
				c.realreferrer,
				COALESCE(COUNT(a.year_week), 0) AS visit
			FROM cross_joined c
			LEFT JOIN agg_on_day a ON c.year_week = a.year_week AND c.realreferrer = a.realreferrer
			GROUP BY c.year_week, c.realreferrer
		),
		count_cte AS (
			SELECT
				t.year_week AS cte_week,
				t.realreferrer,
				COALESCE(visit, 0) AS referrer_count
			FROM summed_data t
			LEFT JOIN distinct_weeks d ON t.year_week = d.year_week
		),
		real__format AS (
			SELECT cte_week AS week_number,
				case 
                WHEN realreferrer like ''NL%'' THEN ''Newsletter''
                else realreferrer
				END AS realreferrer,
				referrer_count AS referrer_count
			FROM count_cte
        ),
		real_format as(
			SELECT 
				week_number,
				realreferrer,
				sum(referrer_count) AS referrer_count
			FROM real__format 
            GROUP BY week_number,realreferrer
		),
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
                17 AS siteid,
                ''Besøg'' AS label, ''Besøg opdelt på trafikkanaler'' AS hint,
                categories AS categories,
                GROUP_CONCAT(realreferrer ORDER BY FIELD(LOWER(realreferrer),  ''Direct'', ''Search'', ''Facebook'', ''Newsletter'' , ''others'' ) SEPARATOR '','') AS series,
                GROUP_CONCAT(referrer_counts ORDER BY FIELD(LOWER(realreferrer),   ''Direct'', ''Search'', ''Facebook'', ''Newsletter'' , ''others'') SEPARATOR '','') AS data
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

