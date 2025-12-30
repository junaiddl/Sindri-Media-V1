{% macro sales_overview_sales_chart_day_dbt() %}
    {% set site = var('site')%}

    CREATE PROCEDURE `overview_sales_chart_day_dbt_{{site}}`()
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
            AND siteid = {{site}}
            and event_action = 'Receipt'
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
            site_id = {{site}}
			AND YEAR(date) = YEAR(CURRENT_DATE())
        GROUP BY 1, 2
    ),
    agg_prev_year AS (
			SELECT SiteID, date, COALESCE(SUM(HITS), 0) AS pre_year
			FROM prod.events et
			WHERE siteid = {{site}}
            AND event_action = 'Receipt'
			AND YEAR(date) = YEAR(CURRENT_DATE()) - 1
			GROUP BY SiteID, date
    ),
    json_data_prev AS (
        SELECT
            COALESCE(ag.site_id, ap.SiteID) AS SiteID,
            "Salg" AS label,
            "Nyhedsbrev sign-ups år til dato grupperet på dag, uge og måned" AS hint_val,
            GROUP_CONCAT(JSON_QUOTE(DATE_FORMAT(ag.date, '%d/%m')) ORDER BY ag.date SEPARATOR ', ') AS categories,
            GROUP_CONCAT(IFNULL(ag.goals, 0) ORDER BY ag.date SEPARATOR ',') AS goals,
			GROUP_CONCAT(COALESCE(ap.pre_year, 0) ORDER BY ag.date SEPARATOR ',') AS pre_year
        FROM
        agg_goal ag
		left join agg_prev_year ap on ap.siteid=ag.site_id and DATE_FORMAT(ap.date, '%m-%d') = DATE_FORMAT(ag.date, '%m-%d')
        WHERE
            ag.site_id = {{site}}
        GROUP BY COALESCE(ag.site_id, ap.SiteID), label, hint_val
    ),
    json_data_curr AS (
        SELECT
            COALESCE(ao.SiteID, ag.site_id) AS SiteID,
            "Salg" AS label,
            "Nyhedsbrev sign-ups år til dato grupperet på dag, uge og måned" AS hint_val,
            GROUP_CONCAT(JSON_QUOTE(DATE_FORMAT(ag.date, '%d/%m')) ORDER BY ag.date SEPARATOR ', ') AS categories,
            GROUP_CONCAT(IFNULL(ao.data, 0) ORDER BY ag.date SEPARATOR ',') AS data,
            GROUP_CONCAT(IFNULL(ag.goals, 0) ORDER BY ag.date SEPARATOR ',') AS goals
        FROM
        agg_goal ag
		LEFT JOIN agg_on_day ao ON ao.SiteID = ag.site_id AND DATE_FORMAT(ao.date, '%m-%d') = DATE_FORMAT(ag.date, '%m-%d')
        WHERE
            ag.site_id = {{site}} AND ag.date <= DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
        GROUP BY COALESCE(ao.SiteID, ag.site_id), label, hint_val
    )
    SELECT
    CONCAT(
        '{',
        '"site":', COALESCE(p.SiteID, c.SiteID, {{site}}), ',',
        '"data":{"label":"', COALESCE(p.label, 'Salg'), '",',
        '"hint":"', COALESCE(p.hint_val, 'Nyhedsbrev sign-ups år til dato grupperet på dag, uge og måned'), '",',
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