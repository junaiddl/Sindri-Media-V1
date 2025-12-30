{% macro sales_overview_sales_chart_month_dbt() %}

    {% set site = var('site')%}
    CREATE PROCEDURE `overview_sales_chart_month_dbt_{{site}}`()
    BEGIN
    SET SESSION group_concat_max_len = 10000;
    WITH agg_on_month AS (
        SELECT
            SiteID,
            month(date) as month,
            DATE_FORMAT(date, '%b') as monthname,
            SUM(HITS) as data
        FROM
            prod.events
        WHERE
            date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1) AND DATE_SUB(CAST(NOW() as date), INTERVAL 1 DAY)
            AND siteid = {{site}}
            AND  event_action = 'Receipt'
        GROUP BY 1, 2, 3
    ),
    agg_goal AS (
        SELECT
            site_id,
            month(date) as month,
            DATE_FORMAT(date, '%b') as monthname,
            SUM(cta_per_day) AS goals
        FROM
            prod.goals
        WHERE
		site_id = {{site}}
		AND YEAR(date) = YEAR(CURRENT_DATE())
        GROUP BY 1, 2,3
    ),
	agg_prev_year AS (
			SELECT SiteID, month(date) AS month,DATE_FORMAT(date, '%b') as monthname,COALESCE(SUM(HITS), 0) AS pre_year
			FROM prod.events dt
			WHERE siteid = {{site}}
            AND  event_action = 'Receipt'
			AND YEAR(date) = YEAR(CURRENT_DATE()) - 1
			GROUP BY 1,2,3
	),
    json_data_prev AS (
        SELECT
            COALESCE(ag.site_id, ap.SiteID) as SiteID,
            "Salg" AS label,
            "Nyhedsbrev sign-ups år til dato grupperet på dag, uge og måned" AS hint,
            GROUP_CONCAT( concat('"',ag.monthname,'"')  order by ag.month  SEPARATOR ', ') AS categories,
            GROUP_CONCAT(IFNULL(ag.goals, 0) order by ag.month SEPARATOR ',') as goals,
			GROUP_CONCAT(coalesce(pre_year, 0) ORDER BY ag.month SEPARATOR ',') AS pre_year
        FROM
            agg_goal ag
		LEFT JOIN agg_prev_year ap ON ag.site_id = ap.SiteID AND ag.month=ap.month
        WHERE
            ag.site_id = {{site}}
        GROUP BY COALESCE(ag.site_id, ap.SiteID), label, hint
    )
    ,
	json_data_curr AS (
        SELECT
            COALESCE(ao.SiteID, ag.site_id) as SiteID,
            "Salg" AS label,
            "Nyhedsbrev sign-ups år til dato grupperet på dag, uge og måned" AS hint,
            GROUP_CONCAT( concat('"',ag.monthname,'"')  order by ag.month  SEPARATOR ', ') AS categories,
            GROUP_CONCAT(IFNULL(ao.data, 0) order by ag.month SEPARATOR ',') as data,
            GROUP_CONCAT(IFNULL(ag.goals, 0) order by ag.month SEPARATOR ',') as goals
        FROM
            agg_goal ag
        LEFT JOIN agg_on_month ao ON ao.siteid = ag.site_id AND ao.month = ag.month
        WHERE
            ag.site_id = {{site}} AND ag.month <= MONTH(DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY))
        GROUP BY COALESCE(ao.SiteID, ag.site_id), label, hint
    )
    SELECT
    CONCAT(
        '{',
        '"site":', COALESCE(p.SiteID, c.SiteID, {{site}}), ',',
        '"data":{"label":"', COALESCE(p.label, 'Salg'), '",',
        '"hint":"', COALESCE(p.hint, 'Nyhedsbrev sign-ups år til dato grupperet på dag, uge og måned'), '",',
        '"categories":', '[', COALESCE(p.categories, ''), '],',
        '"data":', '[', COALESCE(c.data, ''), '],',
        '"data_prev":', '[', COALESCE(p.pre_year, ''), '],',
        '"goals":', '[', COALESCE(p.goals, ''), ']',
        '}}'
    ) AS json_data
    FROM 
        (SELECT {{site}} AS SiteID) AS dummy
        LEFT JOIN json_data_prev p ON p.SiteID = dummy.SiteID
        LEFT JOIN json_data_curr c ON c.SiteID = dummy.SiteID;
    END
{% endmacro %}