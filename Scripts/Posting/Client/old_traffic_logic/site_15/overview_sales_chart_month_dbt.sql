


        
    
    
    CREATE PROCEDURE `overview_sales_chart_month_dbt_15`()
    BEGIN

    SET SESSION group_concat_max_len = 10000;
    WITH agg_on_month AS (
        SELECT
            e.SiteID as SiteID,
            e.date as date,
            SUM(e.HITS) AS data
        FROM
            prod.events e
        WHERE
            e.date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1) AND DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY)
            AND e.siteid = 15
            AND e.event_action = 'Next Click'
        GROUP BY 1, 2
    ),

    pages as(
        SELECT
            p.siteid,
            p.date ,
            COALESCE(SUM(p.unique_pageviews), 0) AS sum_of_pageviews
        FROM
            prod.pages p
        WHERE
            p.date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
            and p.siteid = 15
        GROUP BY p.siteid,2
    ),
    pages_prev as(
        SELECT
            p.siteid,
            p.date ,
            COALESCE(SUM(p.unique_pageviews), 0) AS sum_of_pageviews
        FROM
            prod.pages p
        WHERE
        YEAR(date) = YEAR(CURRENT_DATE()) - 1
            and p.siteid = 15
        GROUP BY p.siteid,2
    ),
    next_clicks as
    (select siteid,
        month(date) month, DATE_FORMAT(date,'%b') as monthname, round(sum(data)/count(*),2) as data
    from 
    (
    select p.date,p.siteid,ROUND(coalesce(h.data,0.0)/coalesce(p.sum_of_pageviews,1.0) * 100) as data  from pages p
    left join agg_on_month h on p.siteid=h.siteid and p.date=h.date
    ) a
    group by 1,2,3
    ),
    agg_goal AS (
        SELECT
            e.site_id,
            month(e.date) as month,
            DATE_FORMAT(date,'%b') as monthname,
            count(*) as days,
            SUM(e.cta_per_day) AS goals
        FROM
            prod.goals e
        WHERE
            e.site_id = 15
            AND YEAR(date) = YEAR(CURRENT_DATE())
        GROUP BY 1, 2, 3
    )
    ,
	agg_prev_year AS (
			SELECT SiteID, date, COALESCE(SUM(HITS), 0) AS pre_year
			FROM prod.events dt
			WHERE siteid = 15
            AND  event_action = 'Next Click'
			AND YEAR(date) = YEAR(CURRENT_DATE()) - 1
			GROUP BY 1,2
	)
    ,
    next_clicks_prev_year as
        (select siteid,
            month(date) month, DATE_FORMAT(date,'%b') as monthname, round(sum(data)/count(*),2) as pre_year
        from 
        (
        select p.date,p.siteid,ROUND(coalesce(h.pre_year,0.0)/coalesce(p.sum_of_pageviews,1.0) * 100) as data  from pages_prev p
        left join agg_prev_year h on p.siteid=h.siteid and DATE_FORMAT(p.date, '%m-%d') = DATE_FORMAT(h.date, '%m-%d')
        ) a
        group by 1,2,3
    ),
    json_data_prev AS (
      	SELECT
            COALESCE(ag.site_id, ap.SiteID) as SiteID,
            "Gns. next click år til dato grupperet på hhv. dag, uge og måned" as hint,
            "Next click (%)" as label,
            GROUP_CONCAT( concat('"', ag.monthname,'"')  order by ag.month  SEPARATOR ', ') AS categories,
            GROUP_CONCAT(Coalesce(ag.goals, 0)/Coalesce(ag.days,0) order by ag.month SEPARATOR ',') as goals,
			GROUP_CONCAT(coalesce(pre_year, 0) ORDER BY ag.month SEPARATOR ',') AS pre_year
        FROM
            agg_goal ag
		LEFT JOIN next_clicks_prev_year ap ON ag.site_id = ap.SiteID AND ag.month=ap.month
        WHERE
            ag.site_id = 15
            GROUP BY COALESCE(ag.site_id, ap.SiteID), label, hint
    ),
    json_data_curr AS (
      	SELECT
            COALESCE(ao.SiteID, ag.site_id) as SiteID,
            "Gns. next click år til dato grupperet på hhv. dag, uge og måned" as hint,
            "Next click (%)" as label,
            GROUP_CONCAT( concat('"', ag.monthname,'"')  order by ag.month  SEPARATOR ', ') AS categories,
            GROUP_CONCAT(IFNULL(ao.data, 0) order by ag.month SEPARATOR ',') as data,
            GROUP_CONCAT(Coalesce(ag.goals, 0)/Coalesce(ag.days,0) order by ag.month SEPARATOR ',') as goals
        FROM
            agg_goal ag
        LEFT JOIN next_clicks ao ON ao.siteid = ag.site_id AND ao.month = ag.month
        WHERE
            ag.site_id = 15 AND ag.month <= MONTH(DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY))
            GROUP BY COALESCE(ao.SiteID, ag.site_id), label, hint
    )
    SELECT 
        CONCAT('{','"site":',p.SiteID,',','"data":{"label":"', p.label,'",'
        '"hint":"', p.hint,'",'
        '"categories":', '[', p.categories, '],',
        '"data":', '[', c.data, '],',
        '"data_prev":', '[', p.pre_year, '],',
        '"goals":', '[', p.goals, ']',
        '}}'
    ) as json_data
    FROM json_data_prev p
    JOIN json_data_curr c
    ON p.SiteID = c.SiteID;
    END


