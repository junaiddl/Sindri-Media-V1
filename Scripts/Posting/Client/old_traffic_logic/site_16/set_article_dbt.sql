


          
        
        
        
        
        CREATE  PROCEDURE `set_article_dbt_16`()
        BEGIN
        SELECT 
            JSON_OBJECT(
                'site', siteid,
                'article_id', id,
                'article_name', title,
                 'category', COALESCE(userneeds, ''), 'sektion', COALESCE(
CASE
  WHEN tags LIKE '%Løn%' THEN REPLACE(tags, 'Løn', 'Løn og konkrete fordele')
  WHEN tags LIKE '%Arbejdsmiljø%' THEN REPLACE(tags, 'Arbejdsmiljø', 'Arbejdsmiljø')
  WHEN tags LIKE '%Arbejdsliv%' THEN REPLACE(tags, 'Arbejdsliv', 'Det moderne arbejdsliv')
  WHEN tags LIKE '%Udannelse%' THEN REPLACE(tags, 'Udannelse', 'Uddannelse')
  WHEN tags LIKE '%Grøn_omstilling%' THEN REPLACE(tags, 'Grøn_omstilling', 'Grøn omstilling')
  WHEN tags LIKE '%Formanden%' THEN REPLACE(tags, 'Formanden', 'Formanden har ordet')
  ELSE 'Others'
END,''),
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
            siteid = 16 and 
            id is not null
             
             ;
        END


