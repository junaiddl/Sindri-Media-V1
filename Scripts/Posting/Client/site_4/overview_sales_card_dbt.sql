


    

    
    

    CREATE  PROCEDURE `overview_sales_card_dbt_4`()
        BEGIN
with curr_year as (
SELECT
    SiteID,
    SUM(hits) AS value
FROM
    prod.events
WHERE
 siteid = 4 and
    date BETWEEN MAKEDATE(EXTRACT(YEAR FROM CURDATE()), 1) AND DATE_SUB(CAST(NOW() AS DATE), INTERVAL 1 DAY)
    AND event_action = 'Newsletter'
GROUP BY SiteId
)
,last_year as (
SELECT
SiteID,sum(hits) as value_last_year
FROM
    prod.events
WHERE
 DATE_FORMAT(STR_TO_DATE(date, '%d-%m-%Y'),'%Y-%m-%d')
 between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year)
  and siteid = 4
  AND event_action = 'Newsletter'
 group by SiteId
),
goal as (
SELECT
Site_ID,sum(cta_per_day) as progressTotal
FROM
    prod.goals
  where site_id = 4 and date
between (SELECT MIN(date) AS MinimumDate
    FROM prod.pages
    WHERE siteid = 4 and YEAR(date) = YEAR(CURRENT_DATE()))  and  (SELECT MAX(date) AS MaximumDate
                  FROM prod.pages
                  WHERE siteid = 4 and YEAR(date) = YEAR(CURRENT_DATE()))
                  
 group by Site_Id
)
 SELECT JSON_OBJECT('site',COALESCE(cy.siteid,ly.siteid),'data',JSON_OBJECT('label', 'Sign-ups', 'hint', 'Nyhedsbrev sign-ups år til dato ift sidste år til dato og ift målsætning','value',COALESCE(value,0),'change',COALESCE(round(((value-value_last_year)/value_last_year)*100,2), 0), 'progressCurrent',value,'progressTotal',progressTotal)) AS json_data from curr_year cy
 left join last_year ly on cy.siteid=ly.siteid
 left join goal g on g.site_id=cy.siteid
;
        END


