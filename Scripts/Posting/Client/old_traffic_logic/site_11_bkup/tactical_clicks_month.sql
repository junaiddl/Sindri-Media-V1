


    






CREATE PROCEDURE `tactical_clicks_months_dbt_11`()
BEGIN
WITH 
curr_30_days_article_published AS (
    SELECT siteid, id
    FROM prod.site_archive_post
    WHERE date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY) 
    AND siteid = 11
    
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

agg_curr_30_days_events AS (
    SELECT 
        e.siteid AS siteid, 
        SUM(e.hits) AS next_clicks
    FROM prod.events e
    JOIN curr_30_days_article_published a ON a.id = e.postid
    WHERE e.date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY) 
    AND e.siteid = 11
    AND e.Event_Action = 'Next Click'
    GROUP BY 1
),

agg_curr_30_days_pages AS
(
	select
		p.siteid,
        sum(p.unique_pageviews) as pageview_sum
	from prod.pages p
    left join curr_30_days_article_published a ON a.id = p.postid
    where p.siteid = 11
    and p.date between DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
),

value_curr_30_days AS
(
	Select
		e.siteid,
		round(e.next_clicks/p.pageview_sum * 100,2) as value
        from agg_curr_30_days_events e
        left join agg_curr_30_days_pages p on e.siteid = p.siteid
),




last_30_days_article_published AS (
    SELECT siteid, id
    FROM prod.site_archive_post
    WHERE date BETWEEN DATE_SUB(NOW(), INTERVAL 61 DAY) AND DATE_SUB(NOW(), INTERVAL 31 DAY) 
    AND siteid = 11
    
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

agg_last_30_days_events AS (
    SELECT 
        e.siteid AS siteid, 
        SUM(e.hits) AS next_clicks_last
    FROM prod.events e
    JOIN last_30_days_article_published a ON a.id = e.postid
    WHERE e.date BETWEEN DATE_SUB(NOW(), INTERVAL 61 DAY) AND DATE_SUB(NOW(), INTERVAL 31 DAY) 
    AND e.siteid = 11
    AND e.Event_Action = 'Next Click'
    GROUP BY 1
),

agg_last_30_days_pages AS
(
	select
		p.siteid,
        sum(p.unique_pageviews) as pageview_sum_last
	from prod.pages p
    left join last_30_days_article_published a ON a.id = p.postid
    where p.siteid = 11
    and p.date between DATE_SUB(NOW(), INTERVAL 61 DAY) AND DATE_SUB(NOW(), INTERVAL 31 DAY)
),

value_last_30_days AS
(
	Select
		e.siteid,
		round(e.next_clicks_last/p.pageview_sum_last * 100,2) as value_last
        from agg_last_30_days_events e
        left join agg_last_30_days_pages p on e.siteid = p.siteid
)

SELECT 
    JSON_OBJECT(
        'site', al.siteid,
        'data', JSON_OBJECT(
            'label', 'Gns. next click (%)',
            'hint', 'Gns. next click på artikler publiceret seneste 30 dage ift. forrige 30 dage',
            'value', COALESCE(value, 0),
            'change', COALESCE(value - value_last, 0),
            'progressCurrent', '',
            'progressTotal', ''
        )
    ) AS json_data
FROM value_curr_30_days al
LEFT JOIN value_last_30_days alb ON al.siteid = alb.siteid
WHERE al.siteid = 11;

END

