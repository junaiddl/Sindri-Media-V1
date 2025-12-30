




    
        
        CREATE  PROCEDURE `overview_pageviews_chart_month_dbt_4`()
        BEGIN

        SET SESSION group_concat_max_len = 10000;
        WITH agg_on_month as (
            SELECT SiteID,month(date) as month,DATE_FORMAT(date,'%b') as monthname,SUM(Unique_pageviews) as data
            FROM prod.daily_totals
            where date
            between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) and siteid = 4
            group by 1,2,3
        ),
        agg_goal as(
            select site_id,month(date) as month, DATE_FORMAT(date,'%b') as monthname, sum(Pageviews_per_day) AS goals from prod.goals
            where site_id = 4 and YEAR(date) = YEAR(CURRENT_DATE())
            group by 1,2,3
        ),
        agg_prev_year AS (
			SELECT SiteID, month(date) AS month, DATE_FORMAT(date,'%b') as monthname, COALESCE(SUM(Unique_pageviews), 0) AS pre_year
			FROM prod.daily_totals dt
			WHERE siteid = 4
			AND YEAR(date) = YEAR(CURRENT_DATE()) - 1
			GROUP BY 1,2,3
		)
        ,
        json_data_prev as( 
           SELECT
            COALESCE(ag.site_id, ap.SiteId) as SiteID,'Sidevisninger' as label,"Sidevisninger år til dato grupperet på hhv. dag, uge og måned"  as hint ,
            GROUP_CONCAT( concat('"',ap.monthname,'"')  order by ag.month  SEPARATOR ', ') AS categories,
            GROUP_CONCAT(coalesce(goals,0) order by ag.month SEPARATOR ',') as goals,
            GROUP_CONCAT(coalesce(pre_year,0) ORDER BY ag.month SEPARATOR ',') AS pre_year
            FROM
                agg_goal ag
            left join agg_prev_year ap on ap.siteid=ag.site_id and ap.month=ag.month
            group by COALESCE(ag.site_id, ap.SiteId),label,hint
        )
        ,
        json_data_curr as( 
           SELECT
            COALESCE(ag.site_id, ao.SiteId) as SiteID,'Sidevisninger' as label,"Sidevisninger år til dato grupperet på hhv. dag, uge og måned"  as hint ,
            GROUP_CONCAT( concat('"',ag.monthname,'"')  order by ag.month  SEPARATOR ', ') AS categories,
            GROUP_CONCAT(coalesce(data,0) order by ag.month SEPARATOR ',') as data,
            GROUP_CONCAT(coalesce(goals,0) order by ag.month SEPARATOR ',') as goals
            FROM
                agg_goal ag
            LEFT JOIN agg_on_month ao ON ao.SiteID = ag.site_id AND ao.month=ag.month
			WHERE ag.month <= MONTH(DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY))
            group by COALESCE(ag.site_id, ao.SiteId),label,hint
        )
        SELECT 
            CONCAT('{','"site":',p.SiteID,',','"data":{"label":"', p.label,'",'
            '"hint":"',  p.hint,'",'
            '"categories":'
            ,'[',p.categories,'],',
            '"data":','[',c.data,'],',
            '"data_prev":','[',p.pre_year,'],',
            '"goals":','[',p.goals,']','}}') as json_data
        FROM json_data_prev p
        JOIN json_data_curr c
        ON p.SiteID = c.SiteID;

        END

 

