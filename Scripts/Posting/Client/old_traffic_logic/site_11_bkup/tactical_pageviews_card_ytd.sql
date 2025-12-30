


    





CREATE PROCEDURE `tactical_pageviews_card_ytd_dbt_11`()
BEGIN
    with last_ytd_article_published as (
        SELECT siteid as siteid,id   FROM prod.site_archive_post
        where 
        date
        between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) and siteid = 11
        
        AND 
(
(tags NOT LIKE "%billedkunst%" 
AND tags NOT LIKE "%børnehaveklassen%" 
AND tags NOT LIKE "%dsa%" 
AND tags NOT LIKE "%engelsk%" 
AND tags NOT LIKE "%håndværk og design%" 
AND tags NOT LIKE "%idræt%" 
AND tags NOT LIKE "%kulturfag%" 
AND tags NOT LIKE "%lærersenior%" 
AND tags NOT LIKE "%lærerstuderende%" 
AND tags NOT LIKE "%madkundskab%" 
AND tags NOT LIKE "%musik%" 
AND tags NOT LIKE "%naturfag%" 
AND tags NOT LIKE "%plc%" 
AND tags NOT LIKE "%ppr%" 
AND tags NOT LIKE "%praktik%" 
AND tags NOT LIKE "%sosu%" 
AND tags NOT LIKE "%tyskfransk%" 
AND tags NOT LIKE "%uu%")
OR 
(tags LIKE "%dansk%" 
OR tags LIKE "%it,%" 
OR tags LIKE ",%it,%" 
OR tags LIKE "%,it%" 
OR tags LIKE "%matematik%" 
OR tags LIKE "%specialpædagogik%" 
OR tags LIKE "%Skolepolitik%" 
OR tags LIKE "%DLF%" 
OR tags LIKE "%Skoleledelse%" 
OR tags LIKE "%Psykisk arbejdsmiljø%" 
OR tags LIKE "%Forskning%"
OR tags LIKE "%Lærerstuderende%")
OR
(tags NOT LIKE "%billedkunst%" 
AND tags NOT LIKE "%børnehaveklassen%" 
AND tags NOT LIKE "%dsa%" 
AND tags NOT LIKE "%engelsk%" 
AND tags NOT LIKE "%håndværk og design%" 
AND tags NOT LIKE "%idræt%" 
AND tags NOT LIKE "%kulturfag%" 
AND tags NOT LIKE "%lærersenior%" 
AND tags NOT LIKE "%lærerstuderende%" 
AND tags NOT LIKE "%madkundskab%" 
AND tags NOT LIKE "%musik%" 
AND tags NOT LIKE "%naturfag%" 
AND tags NOT LIKE "%plc%" 
AND tags NOT LIKE "%ppr%" 
AND tags NOT LIKE "%praktik%" 
AND tags NOT LIKE "%sosu%" 
AND tags NOT LIKE "%tyskfransk%" 
AND tags NOT LIKE "%uu%")
AND 
(tags NOT LIKE "%dansk%" 
AND tags NOT LIKE "%it,%" 
AND tags NOT LIKE ",%it,%" 
AND tags NOT LIKE "%,it%" 
AND tags NOT LIKE "%matematik%" 
AND tags NOT LIKE "%specialpædagogik%" 
AND tags NOT LIKE "%Skolepolitik%" 
AND tags NOT LIKE "%DLF%" 
AND tags NOT LIKE "%Skoleledelse%" 
AND tags NOT LIKE "%Psykisk arbejdsmiljø%" 
AND tags NOT LIKE "%Forskning%")
)

        
    ),
    last_ytd_article_published_Agg as (
        select siteid as siteid,count(*) as agg from  last_ytd_article_published
        where  siteid = 11
        group by 1
    ),
    agg_last_ytd as (
        select  e.siteid as siteid,sum(unique_pageviews)/agg as value from prod.pages e
        join  last_ytd_article_published a on a.id=e.postid
        join last_ytd_article_published_Agg agg on agg.siteid=a.siteid 
        where e.date
        between MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1)  and  DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY) and e.siteid = 11
        group by 1,agg
    ),
    last_ytd_before_article_published as (
        SELECT siteid as siteid,id   FROM prod.site_archive_post
        where 
        DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year) and siteid = 11
        
        AND 
(
(tags NOT LIKE "%billedkunst%" 
AND tags NOT LIKE "%børnehaveklassen%" 
AND tags NOT LIKE "%dsa%" 
AND tags NOT LIKE "%engelsk%" 
AND tags NOT LIKE "%håndværk og design%" 
AND tags NOT LIKE "%idræt%" 
AND tags NOT LIKE "%kulturfag%" 
AND tags NOT LIKE "%lærersenior%" 
AND tags NOT LIKE "%lærerstuderende%" 
AND tags NOT LIKE "%madkundskab%" 
AND tags NOT LIKE "%musik%" 
AND tags NOT LIKE "%naturfag%" 
AND tags NOT LIKE "%plc%" 
AND tags NOT LIKE "%ppr%" 
AND tags NOT LIKE "%praktik%" 
AND tags NOT LIKE "%sosu%" 
AND tags NOT LIKE "%tyskfransk%" 
AND tags NOT LIKE "%uu%")
OR 
(tags LIKE "%dansk%" 
OR tags LIKE "%it,%" 
OR tags LIKE ",%it,%" 
OR tags LIKE "%,it%" 
OR tags LIKE "%matematik%" 
OR tags LIKE "%specialpædagogik%" 
OR tags LIKE "%Skolepolitik%" 
OR tags LIKE "%DLF%" 
OR tags LIKE "%Skoleledelse%" 
OR tags LIKE "%Psykisk arbejdsmiljø%" 
OR tags LIKE "%Forskning%"
OR tags LIKE "%Lærerstuderende%")
OR
(tags NOT LIKE "%billedkunst%" 
AND tags NOT LIKE "%børnehaveklassen%" 
AND tags NOT LIKE "%dsa%" 
AND tags NOT LIKE "%engelsk%" 
AND tags NOT LIKE "%håndværk og design%" 
AND tags NOT LIKE "%idræt%" 
AND tags NOT LIKE "%kulturfag%" 
AND tags NOT LIKE "%lærersenior%" 
AND tags NOT LIKE "%lærerstuderende%" 
AND tags NOT LIKE "%madkundskab%" 
AND tags NOT LIKE "%musik%" 
AND tags NOT LIKE "%naturfag%" 
AND tags NOT LIKE "%plc%" 
AND tags NOT LIKE "%ppr%" 
AND tags NOT LIKE "%praktik%" 
AND tags NOT LIKE "%sosu%" 
AND tags NOT LIKE "%tyskfransk%" 
AND tags NOT LIKE "%uu%")
AND 
(tags NOT LIKE "%dansk%" 
AND tags NOT LIKE "%it,%" 
AND tags NOT LIKE ",%it,%" 
AND tags NOT LIKE "%,it%" 
AND tags NOT LIKE "%matematik%" 
AND tags NOT LIKE "%specialpædagogik%" 
AND tags NOT LIKE "%Skolepolitik%" 
AND tags NOT LIKE "%DLF%" 
AND tags NOT LIKE "%Skoleledelse%" 
AND tags NOT LIKE "%Psykisk arbejdsmiljø%" 
AND tags NOT LIKE "%Forskning%")
)

        
    ),
    last_ytd_before_article_published_Agg as (
        select siteid as siteid,count(*) as agg from  last_ytd_before_article_published
        where siteid = 11
        group by 1
    ),
    agg_last_ytd_before as  (
        select  e.siteid as siteid,sum(unique_pageviews)/agg as value from prod.pages e
        join  last_ytd_before_article_published a on a.id=e.postid
        join last_ytd_before_article_published_Agg agg on agg.siteid=a.siteid 
        where DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year) and e.siteid = 11
        group by 1,agg
    ),
    json_data as ( 
        select al.siteid as site, 'Gns. sidevisninger pr artikel' label, 'Gns. sidevisninger pr artikler publiceret år til dato ift. sidste år til dato'  hint, al.value as valu,((al.value-alb.value)/al.value)*100 as chang, '' as progressCurrent , '' as progresstotal
        from agg_last_ytd al
        left join agg_last_ytd_before alb on al.siteid=alb.siteid
        where al.siteid = 11
    )
    select JSON_OBJECT('site',site,'data',JSON_OBJECT('label', label,'hint',hint,'value',COALESCE(valu,0),'change',COALESCE(chang,0),'progressCurrent',progressCurrent, 'progressTotal',progressTotal
    )) AS json_data  from json_data;
END

