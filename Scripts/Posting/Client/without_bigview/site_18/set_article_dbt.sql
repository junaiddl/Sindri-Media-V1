


          




CREATE  PROCEDURE `set_article_dbt_18`()
BEGIN

SELECT 
    JSON_OBJECT(
        'site', siteid,
        'article_id', id,
        'article_name', title,
        'category', COALESCE(userneeds, ''),
		'sektion',COALESCE(categories, ''),
        'tags',COALESCE(tags, ''),
        'date_posted', date,
        'date_updated',  DATE(Modified),
        'url', link,
        'avatar', '',
        'status_text', '',
        'status_color', ''
    ) AS result
FROM 
    prod.site_archive_post
WHERE 
    siteid = 18 and 
    id is not null
    AND date >= DATE_SUB(CURDATE(), INTERVAL 3 DAY);
   
    
END

