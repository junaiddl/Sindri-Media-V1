{% macro subscription_set_sitestats_dbt() %}  
     
    {% set site = var('site')%}
    {% set historic = var('exploration')['historic_data']%}


    CREATE  PROCEDURE `set_sitestats_dbt_{{site}}`()
        BEGIN
        WITH event_d AS (
            SELECT
                e.SiteID,
                DATE(e.date) AS date,
                SUM(e.HITS) AS sales
            FROM
                prod.events e
            WHERE
                    e.SiteID = {{site}} and
                event_action = 'Receipt'
            {% if historic %}
                        AND e.date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                {% endif %}
            GROUP BY 1, 2
        ),
        daily_d AS (
            SELECT
                d.SiteID AS SiteID,
                DATE(d.date) AS date,
                d.Unique_pageviews AS pageviews,
                d.Visits AS visitors
            FROM
                prod.daily_totals d
            WHERE
                d.SiteID = {{site}}
            {% if historic %}
                        AND d.date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
                {% endif %}
        ),
        goals_d AS (
            SELECT
                g.CTA_per_day AS sales_goal,
                g.Pageviews_per_day AS pageviews_goal,
                g.Visits_per_day AS visitors_goal,
                DATE(g.date) AS date,
                g.site_id
            FROM
                prod.goals g
                where  site_id = {{site}}
        ),
        json_Data AS (
            SELECT
                distinct d.SiteID AS SiteID,
                d.date,
                CAST(sales AS SIGNED) AS sales,
                CAST(sales_goal AS SIGNED) AS sales_goal,
                CAST(pageviews AS SIGNED) AS pageviews,
                CAST(pageviews_goal AS SIGNED) AS pageviews_goal,
                CAST(visitors AS SIGNED) AS visitors,
                CAST(visitors_goal AS SIGNED) AS visitors_goal
            FROM
                daily_d AS d
            left JOIN
                event_d e ON d.date = e.date
            left JOIN
                goals_d AS g ON d.date = g.date and g.site_id=d.siteid
            WHERE
                d.SiteID = {{site}}
            union 
            SELECT
            distinct e.SiteID AS SiteID,
                e.date,
                CAST(sales AS SIGNED) AS sales,
                CAST(sales_goal AS SIGNED) AS sales_goal,
                CAST(pageviews AS SIGNED) AS pageviews,
                CAST(pageviews_goal AS SIGNED) AS pageviews_goal,
                CAST(visitors AS SIGNED) AS visitors,
                CAST(visitors_goal AS SIGNED) AS visitors_goal
            FROM
                daily_d AS d
            right JOIN
                event_d e ON d.date = e.date
            left JOIN
                goals_d AS g ON d.date = g.date and g.site_id=d.siteid
            WHERE
                e.SiteID = {{site}}
            
        )
        SELECT
            JSON_OBJECT(
                'site', SiteID,
                'date', date,
                'sales', coalesce(sales,0),
                'sales_goal', coalesce(sales_goal,0),
                'pageviews', coalesce(pageviews,0),
                'pageviews_goal', coalesce(pageviews_goal,0),
                'visitors', coalesce(visitors,0),
                'visitors_goal', coalesce(visitors_goal,0)
            ) AS result
        FROM
            json_Data;
    END

{% endmacro %}