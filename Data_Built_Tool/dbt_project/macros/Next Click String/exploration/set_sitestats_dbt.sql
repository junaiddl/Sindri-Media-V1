{% macro string_next_click_set_sitestats_dbt() %}  
     
        {% set site = var('site')%}
        {% set event = var('event_action')%}
        {% set historic = var('exploration')['historic_data']%}

        CREATE  PROCEDURE `set_sitestats_dbt_{{site}}`()
        BEGIN
        WITH event_d AS (
            SELECT
                e.SiteID,
                DATE(e.date) AS date,
                SUM(e.HITS) AS sales
            FROM
                prod.events_string e
            WHERE
                e.SiteID = {{site}} AND
                e.event_action =  '{{event}}'
            {% if historic %}
               AND e.date >= DATE_SUB(CURDATE(), INTERVAL 7 DAY)
            {% endif %}
            GROUP BY 1, 2
        ),
        pages AS (
        SELECT
                p.siteid,
                p.date,
                COALESCE(SUM(p.unique_pageviews), 0) AS pageviews,
                COALESCE(dt.visits,0) as visitors
            FROM
                prod.pages_string p
            Left join prod.daily_totals dt on p.date=dt.date and p.siteid=dt.siteid
            WHERE
            {% if historic %}
               p.date>= DATE_SUB(CURDATE(), INTERVAL 7 DAY) and 
            {% endif %}
                p.siteid = {{site}}
            GROUP BY 1,2,4
        
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
            WHERE
                g.site_id = {{site}}
        ),
        json_Data AS (
            SELECT
                DISTINCT d.SiteID AS SiteID,
                d.date,
            ROUND((COALESCE(e.sales, 0) / COALESCE(d.pageviews, 1) * 100),2) AS result,
                CAST(sales_goal AS SIGNED) AS sales_goal,
                CAST(pageviews AS SIGNED) AS pageviews,
                CAST(pageviews_goal AS SIGNED) AS pageviews_goal,
                CAST(visitors AS SIGNED) AS visitors,
                CAST(visitors_goal AS SIGNED) AS visitors_goal
            FROM
                pages AS d
            LEFT JOIN
                event_d e ON d.date = e.date
            LEFT JOIN
                goals_d AS g ON d.date = g.date AND g.site_id = d.siteid
            WHERE
                d.SiteID = {{site}}
            UNION 
            SELECT
                DISTINCT e.SiteID AS SiteID,
                e.date,
            ROUND((COALESCE(e.sales, 0) / COALESCE(d.pageviews, 1) * 100),2) AS result,

                CAST(sales_goal AS SIGNED) AS sales_goal,
                CAST(pageviews AS SIGNED) AS pageviews,
                CAST(pageviews_goal AS SIGNED) AS pageviews_goal,
                CAST(visitors AS SIGNED) AS visitors,
                CAST(visitors_goal AS SIGNED) AS visitors_goal
            FROM
                pages AS d
            RIGHT JOIN
                event_d e ON d.date = e.date
            LEFT JOIN
                goals_d AS g ON d.date = g.date AND g.site_id = d.siteid
            WHERE
                e.SiteID = {{site}}
        ),
        daily_total as (
        select p.siteid,p.date,p.visits as visitors,d.visitors_goal,d.pageviews_goal,p.unique_pageviews as pageviews,d.sales_goal,d.result from prod.daily_totals p  
        join json_Data d on p.siteid=d.siteid and p.date=d.date
        where 
            {% if historic %}
              --  p.date>= DATE_SUB(CURDATE(), INTERVAL 7 DAY) and 
            {% endif %}
            p.siteid = {{site}} 
        )


        SELECT
            JSON_OBJECT(
                'site', SiteID,
                'date', date,
                'sales', COALESCE(result, 0),
                'sales_goal', COALESCE(sales_goal, 0),
                'pageviews', COALESCE(pageviews, 0),
                'pageviews_goal', COALESCE(pageviews_goal, 0),
                'visitors', COALESCE(visitors, 0),
                'visitors_goal', COALESCE(visitors_goal, 0)
            ) AS daily_total
        FROM
            daily_total;

        END

{% endmacro %}