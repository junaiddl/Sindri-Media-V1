


      
     
        
        
        

        CREATE  PROCEDURE `set_sitestats_dbt_17`()
        BEGIN
        WITH event_d AS (
            SELECT
                e.SiteID,
                DATE(e.date) AS date,
                SUM(e.HITS) AS sales
            FROM
                prod.events e
            WHERE
                e.SiteID = 17 AND
                e.event_action =  'Next Click'
            
            GROUP BY 1, 2
        ),
        pages AS (
        SELECT
                p.siteid,
                p.date,
                COALESCE(SUM(p.unique_pageviews), 0) AS pageviews,
                COALESCE(dt.visits,0) as visitors
            FROM
                prod.pages p
            Left join prod.daily_totals dt on p.date=dt.date and p.siteid=dt.siteid
            WHERE
            
                p.siteid = 17
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
                g.site_id = 17
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
                d.SiteID = 17
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
                e.SiteID = 17
        ),
        daily_total as (
        select p.siteid,p.date,p.visits as visitors,d.visitors_goal,d.pageviews_goal,p.unique_pageviews as pageviews,d.sales_goal,d.result from prod.daily_totals p  
        join json_Data d on p.siteid=d.siteid and p.date=d.date
        where 
            
            p.siteid = 17 
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


