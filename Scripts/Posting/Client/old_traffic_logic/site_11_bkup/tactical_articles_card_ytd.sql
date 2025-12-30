


    





CREATE PROCEDURE `tactical_articles_card_ytd_dbt_11`()
BEGIN
  with curr_ytd as(
    SELECT siteId as siteId,count(date) as value FROM prod.site_archive_post
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

    
    group by 1
  ),
  last_ytd as (
    SELECT  siteId as siteId,count(date) value  FROM prod.site_archive_post
    where 
    date 
    between DATE_SUB(MAKEDATE(EXTRACT(YEAR FROM CURDATE()),1), INTERVAL 1 Year)  and  DATE_SUB(DATE_SUB(cast(NOW() as date), INTERVAL 1 DAY),INTERVAL 1 Year)
    and siteid = 11
    
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

    
    group by 1
  ),
  json_data as ( 
    select ld.siteid as siteid,"Artikler" as label,"Artikler publiceret år til dato ift. sidste år til dato" as hint,ld.value as valu,COALESCE(((ld.value-lb.value)/lb.value)*100,0)  as chang,'' as progressCurrent
    ,'' as progressTotal from curr_ytd ld
    left join last_ytd lb
    on ld.siteid=lb.siteid
  )
  select  JSON_OBJECT('site',siteid,'data',JSON_OBJECT('label', label,'hint',hint,'value',COALESCE(valu,0),'change',chang,'progressCurrent',progressCurrent, 'progressTotal',progressTotal
  )) AS json_data  from json_data
  ;
END

