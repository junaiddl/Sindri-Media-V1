



        
    
        
        

        CREATE PROCEDURE `overview_sales_chart_day_dbt_24`()
        BEGIN


        SET SESSION group_concat_max_len = 100000;

        WITH agg_on_day AS (
            SELECT
                e.SiteID as SiteID,
                e.date as date,
                SUM(e.HITS) AS data
            FROM
                prod.events_string e
            WHERE
                e.date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
                AND e.event_action = 'Next Click'
                AND e.siteid = 24
            GROUP BY 1, 2
        ),

        pages as(
            SELECT
                p.siteid as siteid,
                p.date as date,
                COALESCE(SUM(p.unique_pageviews), 0) AS sum_of_pageviews
            FROM
                prod.pages_string p
            WHERE
                p.date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
                and p.siteid = 24
            GROUP BY 1,2
        ),
        agg_goal AS (
            SELECT
                e.site_id as site_id,
                e.date as date,
                SUM(e.cta_per_day) AS goals
            FROM
                prod.goals e
            WHERE
                e.site_id = 24
                AND YEAR(date) = YEAR(CURRENT_DATE())
            GROUP BY 1, 2
        ),
        agg_prev_year AS (
                SELECT SiteID, date, COALESCE(SUM(HITS), 0) AS pre_year
                FROM prod.events_string et
                WHERE siteid = 24
                AND event_action = 'Next Click'
                AND YEAR(date) = YEAR(CURRENT_DATE()) - 1
                GROUP BY SiteID, date
        ),
		 pages_prev as(
					SELECT
						p.siteid as siteid,
						p.date as date,
						COALESCE(SUM(p.unique_pageviews), 0) AS sum_of_pageviews
					FROM
						prod.pages_string p
					WHERE
                    YEAR(date) = YEAR(CURRENT_DATE()) - 1
						and p.siteid = 24
					GROUP BY 1,2
		),
        json_data_prev AS (
             SELECT
                COALESCE(ag.site_id, ap.SiteID) AS SiteID,
                "Next click (%)" AS label,
                'Gns. next click år til dato grupperet på hhv. dag, uge og måned' AS hint,
                GROUP_CONCAT(JSON_QUOTE(DATE_FORMAT(ag.date, '%d/%m')) ORDER BY ag.date SEPARATOR ', ') AS categories,
                GROUP_CONCAT(IFNULL(ag.goals, 0) ORDER BY ag.date SEPARATOR ',') AS goals,
			    GROUP_CONCAT(IFNULL(round(ap.pre_year/pp.sum_of_pageviews *100,2), 0) ORDER BY ag.date SEPARATOR ',') AS pre_year
            FROM
                agg_goal ag
            left join pages_prev pp on pp.siteid = ag.site_id and DATE_FORMAT(pp.date, '%m-%d') = DATE_FORMAT(ag.date, '%m-%d')
		    LEFT JOIN agg_prev_year ap ON ag.site_id = ap.SiteID AND DATE_FORMAT(ag.date, '%m-%d') = DATE_FORMAT(ap.date, '%m-%d')
            WHERE
                ag.site_id = 24
			GROUP BY COALESCE(ag.site_id, ap.SiteID), label, hint
        ),
		json_data_curr AS (
             SELECT
                COALESCE(ao.SiteID, ag.site_id) AS SiteID,
                "Next click (%)" AS label,
                'Gns. next click år til dato grupperet på hhv. dag, uge og måned' AS hint,
                GROUP_CONCAT(JSON_QUOTE(DATE_FORMAT(ag.date, '%d/%m')) ORDER BY ag.date SEPARATOR ', ') AS categories,
                GROUP_CONCAT(IFNULL(round(ao.data/p.sum_of_pageviews *100,2), 0) ORDER BY ag.date SEPARATOR ',') AS data,
                GROUP_CONCAT(IFNULL(ag.goals, 0) ORDER BY ag.date SEPARATOR ',') AS goals
            FROM
                agg_goal ag
            left join pages p on p.siteid = ag.site_id and DATE_FORMAT(p.date, '%m-%d') = DATE_FORMAT(ag.date, '%m-%d')
			LEFT JOIN agg_on_day ao ON ao.siteid = ag.site_id AND DATE_FORMAT(ao.date, '%m-%d') = DATE_FORMAT(ag.date, '%m-%d')
            WHERE
                ag.site_id = 24 AND ag.date <= DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
			GROUP BY SiteID, label, hint
        )
        SELECT 
            CONCAT('{','"site":',p.SiteID,',','"data":{"label":"', p.label,'",'
            '"hint":"', p.hint,'",'
            '"categories":','[', p.categories,'],'
            '"data":', '[', c.data, '],',
            '"data_prev":', '[', p.pre_year, '],',
            '"goals":', '[', p.goals, ']',
            '}}'
        ) as json_data
        FROM json_data_prev p
        JOIN json_data_curr c
        ON p.SiteID = c.SiteID;
        END

