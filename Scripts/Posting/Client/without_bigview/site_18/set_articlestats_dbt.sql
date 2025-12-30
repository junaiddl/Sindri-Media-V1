


       




CREATE  PROCEDURE `set_articlestats_dbt_18`()
BEGIN

WITH event_d AS (
    SELECT
        e.Siteid,
        e.postid,
        DATE(e.date) AS date,
        SUM(HITS) AS clicks
    FROM
        prod.events e
    WHERE
            e.siteid = 18
         AND event_action =  'Sales'
		AND e.date >= DATE_SUB(CURDATE(), INTERVAL 3 DAY)
    and e.postid is  not null
    GROUP BY 1, 2, 3
),
pages_d AS (
    SELECT
        p.Siteid AS siteid,
        PostID AS article_id,
        DATE(p.date) AS date,
        SUM(unique_pageviews) AS brugerbehov
    FROM
        prod.pages p
    WHERE
        p.siteid = 18 and PostID is not null
		and p.date >= DATE_SUB(CURDATE(), INTERVAL 3 DAY)
    GROUP BY 1, 2, 3
   
),
json_Data as(
    SELECT
        p.siteid AS siteid,
        p.date,
        article_id,
        brugerbehov,
        clicks
    FROM
        pages_d AS p
    left JOIN
        event_d e ON p.date = e.date AND p.article_id = e.PostID
        where article_id is  not null
	union 
      SELECT
        e.siteid AS siteid,
        e.date,
        article_id,
        brugerbehov,
        clicks
    FROM
        pages_d AS p
    right JOIN
        event_d e ON p.date = e.date AND p.article_id = e.PostID
        where article_id is  not null
)
SELECT
    JSON_OBJECT(
        'site', CAST(siteid AS SIGNED),
        'date', date,
        'article_id', article_id,
        'brugerbehov', coalesce(CAST(brugerbehov AS SIGNED),0),
        'clicks', ROUND(COALESCE(CAST(clicks AS SIGNED), 0) / COALESCE(CAST(brugerbehov AS SIGNED), 1) * 100)

    ) AS result
FROM
    json_Data;
         
         
END

