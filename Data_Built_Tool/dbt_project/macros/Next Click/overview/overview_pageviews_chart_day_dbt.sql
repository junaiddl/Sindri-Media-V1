{% macro next_click_overview_pageviews_chart_day_dbt() %}    
    {% set site = var('site')%}
    CREATE  PROCEDURE `overview_pageviews_chart_day_dbt_{{site}}`()
    BEGIN

    SET SESSION group_concat_max_len = 10000;
    WITH agg_on_day as (
        SELECT SiteID,date,SUM(Unique_pageviews) as data
        FROM prod.daily_totals
        where date
        between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) and siteid = {{site}}
        group by 1,2
    ),
    agg_goal as(
        select site_id, date,sum(Pageviews_per_day) AS goals from prod.goals
        where  site_id = {{site}} and YEAR(date) = YEAR(CURRENT_DATE())
        group by 1,2
    ),
    agg_prev_year AS (
        SELECT SiteID, date AS date, SUM(Unique_pageviews) AS pre_year
        FROM prod.daily_totals dt
        WHERE siteid = {{site}}
        AND YEAR(date) = YEAR(CURRENT_DATE()) - 1
        GROUP BY SiteID, date
    ),
    json_data_prev as( 
    SELECT
        COALESCE(ag.site_id, ap.SiteID) as SiteID,"Sidevisninger" as label,"Sidevisninger år til dato grupperet på hhv. dag, uge og måned" as hint ,
        GROUP_CONCAT( CONCAT('"',DATE_FORMAT(ag.date ,  '%d/%m'),'"') order by ag.date  SEPARATOR ', ') AS categories,
        GROUP_CONCAT(COALESCE(goals,0) order by ag.date SEPARATOR ',') as goals,
        GROUP_CONCAT(COALESCE(ap.pre_year, 0) ORDER BY ag.date SEPARATOR ',') AS pre_year
        FROM
            agg_goal ag
        left join agg_prev_year ap on ap.siteid=ag.site_id and DATE_FORMAT(ap.date, '%m-%d') = DATE_FORMAT(ag.date, '%m-%d')
        where ag.site_id = {{site}}
        group by COALESCE(ag.site_id, ap.SiteID),label,hint
    )
    ,
    json_data_curr as( 
    SELECT
        COALESCE(ao.SiteID, ag.site_id) as SiteID,"Sidevisninger" as label,"Sidevisninger år til dato grupperet på hhv. dag, uge og måned" as hint ,
        GROUP_CONCAT( CONCAT('"',DATE_FORMAT(ag.date ,  '%d/%m'),'"') order by ag.date  SEPARATOR ', ') AS categories,
        GROUP_CONCAT(COALESCE(data,0) order by ag.date SEPARATOR ',') as data,
        GROUP_CONCAT(COALESCE(goals,0) order by ag.date SEPARATOR ',') as goals
        FROM
            agg_goal ag
        LEFT JOIN agg_on_day ao ON ao.SiteID = ag.site_id AND DATE_FORMAT(ao.date, '%m-%d') = DATE_FORMAT(ag.date, '%m-%d')
        where ag.site_id = {{site}} and ag.date <= DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
        group by COALESCE(ao.SiteID, ag.site_id),label,hint
    )
    SELECT 
        CONCAT('{','"site":',p.SiteID,',','"data":{"label":"', p.label,'",'
        '"hint":"', p.hint,'",'
        '"categories":'
        ,'[',p.categories,'],',
        '"data":','[',c.data,'],',
		'"data_prev":','[',p.pre_year,'],',
        '"goals":','[',p.goals,']','}}') as json_data
    FROM json_data_prev p
    JOIN json_data_curr c
    ON p.SiteID = c.SiteID;
    END
{% endmacro %}