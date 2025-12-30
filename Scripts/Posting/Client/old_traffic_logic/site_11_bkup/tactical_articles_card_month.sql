


    






CREATE PROCEDURE `tactical_articles_card_month_dbt_11`()
BEGIN
    with last_30_day as(
        SELECT siteId as siteId,count(date) as value FROM prod.site_archive_post
        where 
        date between DATE_SUB(NOW(), INTERVAL 31 DAY) and DATE_SUB(NOW(), INTERVAL 1 DAY) and siteid = 11
         
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
    last_30_days_before as (
        SELECT siteId as siteId,count(*) value  FROM prod.site_archive_post
        where 
        date between DATE_SUB(NOW(), INTERVAL 60 DAY) and DATE_SUB(NOW(), INTERVAL 30 DAY) and siteid = 11
        
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
    )

    select 
    JSON_OBJECT(
    'site',ld.siteId,'data',
    JSON_OBJECT('label', "Artikler", 'hint', "Artikler publiceret seneste 30 dage ift. forrige 30 dage",'value',
    COALESCE(ld.value,0),
    'change',COALESCE(round(((ld.value-lb.value)/lb.value)*100,2),0), 
    'progressCurrent','','progressTotal','')) AS json_data
    from last_30_day ld
    left join last_30_days_before lb
    on ld.siteId=lb.siteId;
END

