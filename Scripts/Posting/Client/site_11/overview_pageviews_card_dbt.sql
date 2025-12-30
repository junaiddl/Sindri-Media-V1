



    
    


    CREATE  PROCEDURE `overview_pageviews_card_dbt_11`()
    BEGIN
    with curr_year as (
    SELECT
    SiteID,sum(Unique_pageviews) as value
    FROM
        prod.daily_totals
    WHERE
    date
    between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) and siteid = 11

    group by SiteId
    )
    ,last_year as (
    SELECT
    SiteID,sum(Unique_pageviews) as value_last_year
    FROM
        prod.daily_totals
    WHERE
    date
    between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year)
    and siteid = 11
    group by SiteId
    ),
    goal as (
    SELECT
    Site_ID,sum(Pageviews_per_day) as progressTotal
    FROM
        prod.goals
    where  Site_ID= 11 and date
    between (SELECT MIN(date) AS MinimumDate
        FROM prod.daily_totals
        WHERE siteid =11 and YEAR(date) = YEAR(CURRENT_DATE()))  and  (SELECT MAX(date) AS MaximumDate
                    FROM prod.daily_totals
                    WHERE siteid = 11 and YEAR(date) = YEAR(CURRENT_DATE()))
    group by Site_Id
    ),
    json_data as( 
    select COALESCE(cy.siteid,ly.siteid) as SiteID,"Sidevisninger" as label, "Sidevisnigner år til dato ift sidste år til dato og ift målsætning" as hint,value,COALESCE(round(((value-value_last_year)/value_last_year)*100,2),0) as chang,value as progresscurrent,progressTotal from curr_year cy
    left join last_year ly on cy.siteid=ly.siteid
    left join goal g on g.site_id=cy.siteid

    )

    select JSON_OBJECT('site',SiteID,'data',JSON_OBJECT('label', label,'hint',hint,'change',chang,'value',COALESCE(value,0), 'progressCurrent',progresscurrent,'progressTotal',progressTotal
    )) AS json_data  from json_data;


    END

