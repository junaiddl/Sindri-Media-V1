WITH subscriber_clicks AS (
    SELECT siteid, subscriber_token, COUNT(*) AS total_clicks
    FROM pre_stage.click_events
    WHERE siteid = 14
    GROUP BY siteid, subscriber_token
),
ranked AS (
    SELECT sc.*,  
           RANK() OVER (PARTITION BY siteid ORDER BY total_clicks DESC) AS rnk,
           COUNT(*) OVER (PARTITION BY siteid) AS total_subs
    FROM subscriber_clicks sc
),
engagement_groups AS (
    SELECT siteid, subscriber_token,
           CASE
               WHEN rnk <= CEIL(total_subs * 0.10) THEN 'Høj'
               WHEN total_clicks >= 3 THEN 'Mellem'
               ELSE 'Lav'
           END AS engagement_group
    FROM ranked
),
joined AS (
    SELECT e.engagement_group, c.ref_url, c.created_date, p.id AS article_id,
           p.Title, p.userneeds, p.Categories, p.Tags, p.date, 1 AS clicks
    FROM engagement_groups e
    LEFT JOIN pre_stage.click_events c
        ON c.subscriber_token = e.subscriber_token AND c.siteid = e.siteid
    LEFT JOIN prod.site_archive_post p
        ON p.Link = c.ref_url AND p.siteid = c.siteid
    WHERE c.siteid = 14
),
cleaned AS (
    SELECT engagement_group,
           COALESCE(Title, ref_url) AS article,
           article_id,
           date,
           CASE WHEN TRIM(COALESCE(userneeds, '')) = '' THEN 'Others' ELSE userneeds END AS category,
           CASE WHEN TRIM(COALESCE(Categories, '')) = '' THEN 'Others' ELSE Categories END AS Artikeltype,
           CASE WHEN TRIM(COALESCE(Tags, '')) IN ('Arbejdsmiljø','Social dumping','Ulighed','Privatøkonomi','Fagligt','Uddannelse')
                THEN TRIM(Tags) ELSE 'Others' END AS Emner,
           SUM(clicks) AS brugerbehov
    FROM joined
    GROUP BY engagement_group, article_id, Title, ref_url, userneeds, Categories, Tags, date
),
numbered_entrypages AS (
    SELECT engagement_group, article, category, date, brugerbehov, Artikeltype, Emner,
           CASE WHEN article_id IS NULL THEN ROW_NUMBER() OVER(PARTITION BY engagement_group ORDER BY brugerbehov DESC)
                ELSE article_id END AS id
    FROM cleaned
),
all_categories_first AS (
    SELECT 'Opdater mig' AS category
    UNION ALL SELECT 'Forbind mig'
    UNION ALL SELECT 'Hjælp mig med at forstå'
    UNION ALL SELECT 'Giv mig en fordel'
    UNION ALL SELECT 'Underhold mig'
    UNION ALL SELECT 'Inspirer mig'
    UNION ALL SELECT 'Others'
),
engagement_groups_first AS (
    SELECT DISTINCT engagement_group
    FROM numbered_entrypages
),
all_combinations_first AS (
    SELECT e.engagement_group, c.category
    FROM engagement_groups_first e
    CROSS JOIN all_categories_first c
),aggregated_first AS (
    SELECT 
        engagement_group, 
        category,
        SUM(brugerbehov) AS total_visits
    FROM numbered_entrypages
    GROUP BY engagement_group, category
),
ordered_userneeds AS (

    SELECT 
        a.engagement_group,
        a.category,
        COALESCE(ag.total_visits, 0) AS total_visits,
        COALESCE(
            ROUND(COALESCE(ag.total_visits,0) * 100.0 / SUM(COALESCE(ag.total_visits,0)) 
                  OVER(PARTITION BY a.engagement_group), 1),
            0
        ) AS percentage,
        ROW_NUMBER() OVER (PARTITION BY a.engagement_group ORDER BY a.category) AS tag_order
    FROM all_combinations_first a
    LEFT JOIN aggregated_first ag
        ON a.engagement_group = ag.engagement_group AND a.category = ag.category
),
  pivoted_data_first AS (
SELECT
        engagement_group,
        CONCAT('[', 
               GROUP_CONCAT(DISTINCT CONCAT('"', category, '"') 
                            ORDER BY FIELD(category, 'Opdater mig','Forbind mig','Hjælp mig med at forstå','Giv mig en fordel','Underhold mig','Inspirer mig', 'Others')), 
               ']') AS categories,
        
        GROUP_CONCAT(total_visits 
                     ORDER BY FIELD(category, 'Opdater mig','Forbind mig','Hjælp mig med at forstå','Giv mig en fordel','Underhold mig','Inspirer mig', 'Others')) AS total_visits,
        
        CONCAT('[', 
               GROUP_CONCAT(percentage 
                            ORDER BY FIELD(category, 'Opdater mig','Forbind mig','Hjælp mig med at forstå','Giv mig en fordel','Underhold mig','Inspirer mig', 'Others')), 
               ']') AS percentile
    FROM ordered_userneeds
    GROUP BY engagement_group
    ORDER BY FIELD(engagement_group, 'lav', 'mellem', 'høj')
		),
 final_first as(
		SELECT 	
    'Kliks ift. indhold og engagementsgruppe' AS label,
    categories AS cat, 
    CONCAT('[', 
           GROUP_CONCAT(
               CONCAT('{"name": "', engagement_group, '","data":', percentile, '}')
           ), 
           ']'
    ) AS series
FROM pivoted_data_first
GROUP BY label, cat
),
all_categories_second AS (
    SELECT 'Artikel' AS category
    UNION ALL SELECT 'Debat'
    UNION ALL SELECT 'Guide'
    UNION ALL SELECT 'Nyhedsoverblik'
    UNION ALL SELECT 'Others'
    UNION ALL SELECT 'Digidoc'
),
engagement_groups_second AS (
    SELECT DISTINCT engagement_group
    FROM numbered_entrypages
),
all_combinations_second AS (
    SELECT e.engagement_group, c.category
    FROM engagement_groups_second e
    CROSS JOIN all_categories_second c
),aggregated_second AS (
    SELECT 
        engagement_group, 
        Artikeltype AS category,
        SUM(brugerbehov) AS total_visits
    FROM numbered_entrypages
    GROUP BY engagement_group, Artikeltype
),
ordered_artikeltype AS (
    SELECT 
        a.engagement_group,
        a.category,
        COALESCE(ag.total_visits, 0) AS total_visits,
        COALESCE(
            ROUND(COALESCE(ag.total_visits,0) * 100.0 / SUM(COALESCE(ag.total_visits,0)) 
                  OVER(PARTITION BY a.engagement_group), 1),
            0
        ) AS percentage,
        ROW_NUMBER() OVER (PARTITION BY a.engagement_group ORDER BY a.category) AS tag_order
    FROM all_combinations_second a
    LEFT JOIN aggregated_second ag
        ON a.engagement_group = ag.engagement_group AND a.category = ag.category
),

     pivoted_data_second AS (
			SELECT
        engagement_group,
        CONCAT('[', 
               GROUP_CONCAT(DISTINCT CONCAT('"', category, '"') 
                            ORDER BY FIELD(category, 'Debat','Artikel','Nyhedsoverblik','Guide','Digidoc', 'Others')), 
               ']') AS categories,
        
        GROUP_CONCAT(total_visits 
                     ORDER BY FIELD(category, 'Debat','Artikel','Nyhedsoverblik','Guide','Digidoc', 'Others')) AS total_visits,
        
        CONCAT('[', 
               GROUP_CONCAT(percentage 
                            ORDER BY FIELD(category, 'Debat','Artikel','Nyhedsoverblik','Guide','Digidoc', 'Others')), 
               ']') AS percentile
    FROM ordered_artikeltype
    GROUP BY engagement_group
    ORDER BY FIELD(engagement_group, 'lav', 'mellem', 'høj')
				
),
final_second as(
        SELECT 	
    'Kliks ift. indhold og engagementsgruppe' AS label,
    categories AS cat, 
    CONCAT('[', 
           GROUP_CONCAT(
               CONCAT('{"name": "', engagement_group, '","data":', percentile, '}')
           ), 
           ']'
    ) AS series
FROM pivoted_data_second
GROUP BY label, cat
),
all_categories_third AS (
    SELECT 'Arbejdsmiljø' AS category
    UNION ALL SELECT 'Social dumping'
    UNION ALL SELECT 'Ulighed'
    UNION ALL SELECT 'Privatøkonomi'
    UNION ALL SELECT 'Fagligt'
    UNION ALL SELECT 'Uddannelse'
    UNION ALL SELECT 'Others'
),
engagement_groups_third AS (
    SELECT DISTINCT engagement_group
    FROM numbered_entrypages
),
all_combinations_third AS (
    SELECT e.engagement_group, c.category
    FROM engagement_groups_third e
    CROSS JOIN all_categories_third c
),aggregated_third AS (
    SELECT 
        engagement_group, 
        Emner AS category,
        SUM(brugerbehov) AS total_visits
    FROM numbered_entrypages
    GROUP BY engagement_group, Emner
),
ordered_emner AS (
    SELECT 
        a.engagement_group,
        a.category,
        COALESCE(ag.total_visits, 0) AS total_visits,
        COALESCE(
            ROUND(COALESCE(ag.total_visits,0) * 100.0 / SUM(COALESCE(ag.total_visits,0)) 
                  OVER(PARTITION BY a.engagement_group), 1),
            0
        ) AS percentage,
        ROW_NUMBER() OVER (PARTITION BY a.engagement_group ORDER BY a.category) AS tag_order
    FROM all_combinations_third a
    LEFT JOIN aggregated_third ag
        ON a.engagement_group = ag.engagement_group AND a.category = ag.category
),
     pivoted_data_third AS (
            SELECT
        engagement_group,
        CONCAT('[', 
               GROUP_CONCAT(DISTINCT CONCAT('"', category, '"') 
                            ORDER BY FIELD(category, 'Arbejdsmiljø','Social dumping','Ulighed','Privatøkonomi','Fagligt','Uddannelse', 'Others')), 
               ']') AS categories,
        
        GROUP_CONCAT(total_visits 
                     ORDER BY FIELD(category, 'Arbejdsmiljø','Social dumping','Ulighed','Privatøkonomi','Fagligt','Uddannelse', 'Others')) AS total_visits,
        
        CONCAT('[', 
               GROUP_CONCAT(percentage 
                            ORDER BY FIELD(category, 'Arbejdsmiljø','Social dumping','Ulighed','Privatøkonomi','Fagligt','Uddannelse', 'Others')), 
               ']') AS percentile
    FROM ordered_emner
    GROUP BY engagement_group
    ORDER BY FIELD(engagement_group, 'lav', 'mellem', 'høj')
                
),
final_third as(
        SELECT 	
    'Kliks ift. indhold og engagementsgruppe' AS label,
    categories AS cat, 
    CONCAT('[', 
           GROUP_CONCAT(
               CONCAT('{"name": "', engagement_group, '","data":', percentile, '}')
           ), 
           ']'
    ) AS series
FROM pivoted_data_third
GROUP BY label, cat
) ,
table_json AS (
    SELECT JSON_OBJECT(
        'label', e1.engagement_group,
        'columns', JSON_ARRAY(
            JSON_OBJECT('field', 'id', 'label', 'ID'),
            JSON_OBJECT('field', 'article', 'label', 'TITEL'),
            JSON_OBJECT('field', 'category', 'label', 'BRUGERBEHOV'),
            JSON_OBJECT('field', 'Artikeltype', 'label', 'ARTIKELTYPE','hidden', true),
            JSON_OBJECT('field', 'Emner', 'label', 'EMNER','hidden', true),
            JSON_OBJECT('field', 'date', 'label', 'DATO'),
            JSON_OBJECT('field', 'brugerbehov', 'label', 'KLIK')
        ),
        'rows', (
            SELECT JSON_ARRAYAGG(row_json)
            FROM (
                SELECT JSON_OBJECT(
                    'id', ne2.id,
                    'article', ne2.article,
                    'category', ne2.category,
                    'Artikeltype', ne2.Artikeltype,
                    'Emner', ne2.Emner,
                    'date', ne2.date,
                    'brugerbehov', ne2.brugerbehov
                ) AS row_json
                FROM numbered_entrypages ne2
                WHERE ne2.engagement_group = e1.engagement_group
                ORDER BY ne2.brugerbehov DESC
            ) AS ordered_ne2
        )
    ) AS engagement_json
    FROM (
        SELECT DISTINCT engagement_group
        FROM numbered_entrypages
        ORDER BY FIELD(engagement_group,'Lav','Mellem','Høj')
    ) as e1
),
bar_chart_json AS (
SELECT JSON_OBJECT(
    'defaultTitle', 'Brugerbehov',
    'label', jd.label,
    'categories', CAST(jd.cat AS JSON),
    'series', CAST(jd.series AS JSON),
    'additional', JSON_ARRAY(
        
        JSON_OBJECT(
            'title', 'Artikeltype',
            'data', JSON_OBJECT(
                'label', final_second.label,
                'categories', CAST(final_second.cat AS JSON),
                'series', CAST(final_second.series AS JSON)
            )
        ),
        
        JSON_OBJECT(
            'title', 'Emner',
            'data', JSON_OBJECT(
                'label', final_third.label,
                'categories', CAST(final_third.cat AS JSON),
                'series', CAST(final_third.series AS JSON)
            )
        )
    )
) AS json_data
FROM final_first jd
CROSS JOIN final_second
CROSS JOIN final_third
),
final_json AS (
   select 
   JSON_OBJECT(
    "site",14,
    "data", JSON_OBJECT(
        "entrypages", (
            SELECT JSON_ARRAYAGG(engagement_json)
            FROM table_json
        ),
        "pageviews", (
            SELECT json_data
            FROM bar_chart_json
        )
    ) 

   ) as final_json
)
select * from final_json;








