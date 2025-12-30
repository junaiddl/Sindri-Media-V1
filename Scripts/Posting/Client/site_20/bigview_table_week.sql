



    
    
    

CREATE PROCEDURE `bigview_table_week_dbt_20`()
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
                e.siteid = 20
            AND event_action =  'Next Click'
            AND date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
            
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
            p.siteid = 20 and PostID is not null
            AND date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
        
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
    ),
    next_click_data as(
        SELECT
            siteid AS siteid,
            article_id,
            date,
            brugerbehov AS brugerbehov,
            ROUND(COALESCE(CAST(clicks AS SIGNED), 0) / COALESCE(CAST(brugerbehov AS SIGNED), 1) * 100) AS clicks
		FROM json_Data
    ),
	json_Data_summed as(
        SELECT
            siteid AS siteid,
            article_id,
            SUM(brugerbehov) AS brugerbehov,
            SUM(clicks) / COUNT(clicks) AS clicks
		FROM next_click_data
        GROUP BY 1, 2
    ),
    top_four AS (
        SELECT sap.id, sap.date, IFNULL(ROUND(clicks,1),0) AS clicks, 
        sap.title, IFNULL(sap.userneeds, '') AS userneeds, coalesce(CAST(brugerbehov AS SIGNED),0) AS pageviews
        FROM prod.site_archive_post sap
        JOIN json_Data_summed jd ON sap.id = jd.article_id AND sap.siteid = jd.siteid
        ORDER BY pageviews DESC
        LIMIT 4
        
    ),
    table_data_json AS (
        SELECT CONCAT('[', GROUP_CONCAT(JSON_OBJECT(
            'id', id,
            'article', title,
                'category', COALESCE(userneeds, ''),
            'date', date,
            'clicks', clicks,
            'pageviews', pageviews
        )), ']') AS result 
        FROM top_four
    )
    
        SELECT CONCAT('{
  "site": 20,
  "data": {
    "rows":', result,
    ',
    "columns": [
      {
        "field": "article",
        "label": "TITEL"
      },
      {
        "field": "category",
        "label": "BRUGERBEHOV"
      },
      {
        "field": "date",
        "label": "DATO"
      },
      {
        "field": "clicks",
        "label": "NEXT CLICK (%)"
      },
      {
        "field": "pageviews",
        "label": "SIDEVISNINGER"
      }
    ]
  }
}') AS JSON_DATA
FROM table_data_json;
END


    
    