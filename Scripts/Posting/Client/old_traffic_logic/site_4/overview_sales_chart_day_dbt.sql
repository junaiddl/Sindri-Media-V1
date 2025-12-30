


    
    
    

    CREATE PROCEDURE `overview_sales_chart_day_dbt_4`()
    BEGIN

    SET SESSION group_concat_max_len = 10000;

    WITH agg_on_day AS (
        SELECT
            SiteID,
            date,
            SUM(HITS) AS data
        FROM
            prod.events
        WHERE
            date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
            AND siteid = 4
            and event_action = 'Newsletter'
        GROUP BY 1, 2
    ),
    agg_goal AS (
        SELECT
            site_id,
            date,
            SUM(cta_per_day) AS goals
        FROM
            prod.goals
        WHERE
            site_id = 4
			AND YEAR(date) = YEAR(CURRENT_DATE())
        GROUP BY 1, 2
    ),
    agg_prev_year AS (
			SELECT SiteID, date, COALESCE(SUM(HITS), 0) AS pre_year
			FROM prod.events et
			WHERE siteid = 4
            AND event_action = 'Newsletter'
			AND YEAR(date) = YEAR(CURRENT_DATE()) - 1
			GROUP BY SiteID, date
	),
    json_data_prev AS (
        SELECT
            COALESCE(ag.site_id, ap.SiteID) AS SiteID,
            "Sign-ups" AS label,
            "Nyhedsbrev sign-ups år til dato grupperet på dag, uge og måned" AS hint_val,
            GROUP_CONCAT(JSON_QUOTE(DATE_FORMAT(ag.date, '%d/%m')) ORDER BY ag.date SEPARATOR ', ') AS categories,
            GROUP_CONCAT(IFNULL(ag.goals, 0) ORDER BY ag.date SEPARATOR ',') AS goals,
			GROUP_CONCAT(COALESCE(ap.pre_year, 0) ORDER BY ag.date SEPARATOR ',') AS pre_year
        FROM
        agg_goal ag
		left join agg_prev_year ap on ap.siteid=ag.site_id and DATE_FORMAT(ap.date, '%m-%d') = DATE_FORMAT(ag.date, '%m-%d')
        WHERE
            ag.site_id = 4
        GROUP BY COALESCE(ag.site_id, ap.SiteID), label, hint_val
    ),
    json_data_curr AS (
        SELECT
            COALESCE(ao.SiteID, ag.site_id) AS SiteID,
            "Sign-ups" AS label,
            "Nyhedsbrev sign-ups år til dato grupperet på dag, uge og måned" AS hint_val,
            GROUP_CONCAT(JSON_QUOTE(DATE_FORMAT(ag.date, '%d/%m')) ORDER BY ag.date SEPARATOR ', ') AS categories,
            GROUP_CONCAT(IFNULL(ao.data, 0) ORDER BY ag.date SEPARATOR ',') AS data,
            GROUP_CONCAT(IFNULL(ag.goals, 0) ORDER BY ag.date SEPARATOR ',') AS goals
        FROM
        agg_goal ag
		LEFT JOIN agg_on_day ao ON ao.SiteID = ag.site_id AND DATE_FORMAT(ao.date, '%m-%d') = DATE_FORMAT(ag.date, '%m-%d')
        WHERE
            ag.site_id = 4 AND ag.date <= DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
        GROUP BY COALESCE(ao.SiteID, ag.site_id), label, hint_val
    )
    SELECT
        CONCAT(
            '{',
            '"site":',
            p.SiteID,
            ',',
            '"data":{"label":"',
            p.label,'",'
            '"hint":"',
            p.hint_val,'",'
            '"categories":',
            '[',
            p.categories,
            '],',
            '"data":',
            '[',
            c.data,
            '],',
            '"data_prev":', '[', p.pre_year, '],',
            '"goals":',
            '[',
            p.goals,
            ']',
            '}}'
        ) AS json_data
    FROM
        json_data_prev p
        JOIN json_data_curr c
        ON p.SiteID = c.SiteID;
    END


