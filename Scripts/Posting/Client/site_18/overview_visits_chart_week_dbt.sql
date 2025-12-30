


    

  
    CREATE  PROCEDURE `overview_visits_chart_week_dbt_18`()
    BEGIN
    SET SESSION group_concat_max_len = 10000;
    WITH agg_on_week as (
      SELECT SiteID,case when cast(YEARWEEK(date, 3) as char) like CONCAT('%',CAST(YEAR(DATE) AS CHAR),'%') 
      THEN week(date,3) ELSE 1 end as week,SUM(Visits) as data
      FROM prod.daily_totals
      where date 
      between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) and siteid =18
      group by 1,2
    ),
    agg_goal as(
      select site_id,case when cast(YEARWEEK(date, 3) as char) like CONCAT('%',CAST(YEAR(DATE) AS CHAR),'%') 
      THEN week(date,3) ELSE 1 end as week,sum(Visits_per_day) AS goals from prod.goals
        where site_id = 18
		AND YEAR(date) = YEAR(CURRENT_DATE())
      group by 1,2
    ),
    agg_prev_year AS (
			SELECT SiteID, case when cast(YEARWEEK(date, 3) as char) like CONCAT('%',CAST(YEAR(DATE) AS CHAR),'%') 
            THEN week(date,3) ELSE 1 end as week, COALESCE(SUM(Visits), 0) AS pre_year
			FROM prod.daily_totals dt
			WHERE siteid = 18
			AND YEAR(date) = YEAR(CURRENT_DATE()) - 1
			GROUP BY 1,2
	  ),
    json_data_prev as( 
		 SELECT
		  ag.site_id as SiteID,'Besøg' as label,'Besøg år til dato grupperet på hhv. dag, uge og måned'  as hint ,
		  GROUP_CONCAT( CONCAT('"',ag.week,'"')  order by ag.week  SEPARATOR ', ') AS categories,
		  GROUP_CONCAT(COALESCE(goals,0) order by ag.week SEPARATOR ',') as goals,
			GROUP_CONCAT(coalesce(ap.pre_year,0) ORDER BY ag.week SEPARATOR ',') AS pre_year
		  FROM
			  agg_goal ag
		  left join agg_prev_year ap on ap.siteid=ag.site_id and ap.week=ag.week
		  group by ag.site_id,label,hint
		),
		json_data_curr as( 
		 SELECT
		  ag.site_id as SiteID,'Besøg' as label,'Besøg år til dato grupperet på hhv. dag, uge og måned'  as hint ,
		  GROUP_CONCAT( CONCAT('"',ag.week,'"')  order by ag.week  SEPARATOR ', ') AS categories,
		  GROUP_CONCAT(coalesce(data,0) order by ag.week SEPARATOR ',') as data,
		  GROUP_CONCAT(COALESCE(goals,0) order by ag.week SEPARATOR ',') as goals
		  FROM
			  agg_goal ag
			LEFT JOIN agg_on_week ao ON ao.SiteID = ag.site_id AND ao.week=ag.week
            WHERE ag.week <= CAST(WEEK(DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY), 3) as char)
		  group by ag.site_id,label,hint
		)
      SELECT 
        CONCAT('{','"site":',p.SiteID,',','"data":{"label":"', p.label,'",'
        '"hint":"', p.hint,'",'
        '"categories":'
        ,'[',p.categories,'],',
        '"data":','[',c.data,'],',
        '"data_prev":', '[', p.pre_year, '],',
        '"goals":','[',p.goals,']','}}') as json_data
      FROM json_data_curr c
      JOIN json_data_prev p
      ON c.SiteID = p.SiteID;
    END


