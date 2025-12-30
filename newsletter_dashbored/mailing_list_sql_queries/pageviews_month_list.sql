WITH click_counts AS (
SELECT siteid, subscriber_token, COUNT(*) AS clicks
FROM pre_stage.click_events
WHERE siteid = 14
AND created_date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
and mailing_list = 1
group by 1,2
),
ranked AS (
    SELECT sc.*,  
           RANK() OVER (PARTITION BY siteid ORDER BY clicks DESC) AS rnk,
           COUNT(*) OVER (PARTITION BY siteid) AS total_subs
    FROM click_counts sc
),
engagement_groups AS (
    SELECT siteid, subscriber_token,
            clicks,
           CASE
			   WHEN rnk <= CEIL(total_subs * 0.10) THEN 'Høj'
			   WHEN clicks BETWEEN 1 AND 2 THEN 'Lav'
               WHEN clicks >= 3 THEN 'Mellem'
               else 'Ingen'
           END AS engagement_group
    FROM ranked
),
who_is_sub as (
SELECT 
distinct 
created_at, site_id, 
subscriber_token, subscriber_group,
subscriber_sindri,subscriber_gender,
subscriber_zip,subscriber_position_of_trust 
FROM pre_stage.whoissubscribed_data  where created_at = CURDATE() - INTERVAL 2 Day
and site_id =14 and list_id=1
),
subs AS (
  SELECT
    sd.site_id as siteid,
    sd.subscriber_token,
	subscriber_group,
	 subscriber_sindri,
    subscriber_gender,
    subscriber_position_of_trust,
  
	 subscriber_zip,
  cc.engagement_group
    
  FROM who_is_sub sd
  LEFT JOIN engagement_groups cc
    ON sd.site_id = cc.siteid AND sd.subscriber_token = cc.subscriber_token
  WHERE sd.site_id = 14
) ,
engaged AS (
  SELECT
    siteid,
    subscriber_token,
    subscriber_group,
    subscriber_sindri,
    subscriber_gender,
    subscriber_position_of_trust,
    subscriber_zip,
    engagement_group
  FROM subs
),
all_groups AS (
  SELECT 'Ingen' AS engagement_group UNION ALL
  SELECT 'Lav' UNION ALL
  SELECT 'Mellem' UNION ALL
  SELECT 'Høj'
),

-- 5️⃣ MEDLEM counts and percentages
subscriber_group AS (
  SELECT engagement_group,
         CASE
         WHEN subscriber_group = 'Ukendt gruppe' THEN 'Ukendt gruppe'
         ELSE 'others'
         END AS category,
         COUNT(DISTINCT subscriber_token) AS cnt
  FROM engaged
  GROUP BY engagement_group, category
),
subscriber_group_totals AS (
  SELECT engagement_group, SUM(cnt) AS total
  FROM subscriber_group
  GROUP BY engagement_group
) ,
subscriber_group_cross AS (
  SELECT g.engagement_group, c.category
  FROM all_groups g
  CROSS JOIN (SELECT 'Ukendt gruppe' AS category UNION ALL SELECT 'others') c
),
pct_subscriber_group AS (
  SELECT
    mc.engagement_group,
    mc.category,
    ROUND(
      CASE WHEN mt.total = 0 OR mt.total IS NULL THEN 0
           ELSE 100 * COALESCE(mb.cnt, 0) / mt.total END, 2
    ) AS pct
  FROM subscriber_group_cross mc
  LEFT JOIN subscriber_group mb
    ON mb.engagement_group = mc.engagement_group AND mb.category = mc.category
  LEFT JOIN subscriber_group_totals mt
    ON mt.engagement_group = mc.engagement_group
),

-- 6️⃣ TYPE counts and percentages

subscriber_sindri AS (
  SELECT engagement_group,
   CASE
         WHEN subscriber_sindri = 'mail-a' THEN '<47'
            WHEN subscriber_sindri = 'mail-b' THEN '48-60'
            WHEN subscriber_sindri = 'mail-c' THEN '>60'
           ELSE 'others'
         END AS category,
  
  COUNT(DISTINCT subscriber_token) AS cnt
  FROM engaged 
  GROUP BY engagement_group, category
),
subscriber_sindri_totals AS (
  SELECT engagement_group, SUM(cnt) AS total
  FROM subscriber_sindri
  GROUP BY engagement_group
),
subscriber_sindri_cross AS (
  SELECT g.engagement_group, u.category
  FROM all_groups g CROSS JOIN (
    SELECT '<47' AS category UNION ALL
    SELECT '48-60' UNION ALL
    SELECT '>60'  UNION ALL
    Select 'others'
  ) u
),
pct_subscriber_sindri AS (
  SELECT
    tc.engagement_group,
    tc.category,
    ROUND(
      CASE WHEN tt.total = 0 OR tt.total IS NULL THEN 0
           ELSE 100 * COALESCE(tb.cnt, 0) / tt.total END, 2
    ) AS pct
  FROM subscriber_sindri_cross tc
  LEFT JOIN subscriber_sindri tb
    ON tb.engagement_group = tc.engagement_group AND tb.category = tc.category
  LEFT JOIN subscriber_sindri_totals tt
    ON tt.engagement_group = tc.engagement_group
),
subscriber_gender AS (
  SELECT engagement_group,
COALESCE (subscriber_gender, 'others') AS category,
  COUNT(DISTINCT subscriber_token) AS cnt
  FROM engaged 
  GROUP BY engagement_group, category
),
subscriber_gender_totals AS (
  SELECT engagement_group, SUM(cnt) AS total
  FROM subscriber_gender
  GROUP BY engagement_group
),
subscriber_gender_cross AS (
  SELECT g.engagement_group, u.category
  FROM all_groups g CROSS JOIN (SELECT 'Mand' AS category UNION ALL SELECT 'Kvinde' UNION ALL SELECT 'others') u
),
pct_subscriber_gender AS (
  SELECT
    tc.engagement_group,
    tc.category,
    ROUND(
      CASE WHEN tt.total = 0 OR tt.total IS NULL THEN 0
           ELSE 100 * COALESCE(tb.cnt, 0) / tt.total END, 2
    ) AS pct
  FROM subscriber_gender_cross tc
  LEFT JOIN subscriber_gender tb
    ON tb.engagement_group = tc.engagement_group AND tb.category = tc.category
  LEFT JOIN subscriber_gender_totals tt
    ON tt.engagement_group = tc.engagement_group
),

subscriber_position_of_trust AS (
  SELECT engagement_group,
COALESCE (subscriber_position_of_trust, 'others') AS category,
  COUNT(DISTINCT subscriber_token) AS cnt
  FROM engaged 
  GROUP BY engagement_group, category
),
subscriber_position_of_trust_totals AS (
  SELECT engagement_group, SUM(cnt) AS total
  FROM subscriber_position_of_trust
  GROUP BY engagement_group
),
subscriber_position_of_trust_cross AS (
  SELECT g.engagement_group, u.category
  FROM all_groups g CROSS JOIN (select "Nej" AS category UNION ALL select "Ja" UNION ALL select "others") u
),
pct_subscriber_position_of_trust AS (
  SELECT
    tc.engagement_group,
    tc.category,
    ROUND(
      CASE WHEN tt.total = 0 OR tt.total IS NULL THEN 0
           ELSE 100 * COALESCE(tb.cnt, 0) / tt.total END, 2
    ) AS pct
  FROM subscriber_position_of_trust_cross tc
  LEFT JOIN subscriber_position_of_trust tb
    ON tb.engagement_group = tc.engagement_group AND tb.category = tc.category
  LEFT JOIN subscriber_position_of_trust_totals tt
    ON tt.engagement_group = tc.engagement_group
),

subscriber_zip AS (
  SELECT engagement_group,

    CASE
    WHEN subscriber_zip BETWEEN 0000 AND 3999 THEN 'Hovedstaden'
    WHEN subscriber_zip BETWEEN 4000 AND 4999 THEN 'Sjælland'
    WHEN subscriber_zip BETWEEN 5000 AND 6999 THEN 'Syddanmark'
    WHEN subscriber_zip BETWEEN 7000 AND 8999 THEN 'Midtjylland'
    WHEN subscriber_zip BETWEEN 9000 AND 9999 THEN 'Nordjylland'
    ELSE 'others'
    END AS category,
  
  COUNT(DISTINCT subscriber_token) AS cnt
  FROM engaged 
  GROUP BY engagement_group, category
),
subscriber_zip_totals AS (
  SELECT engagement_group, SUM(cnt) AS total
  FROM subscriber_zip
  GROUP BY engagement_group
),
subscriber_zip_cross AS (
  SELECT g.engagement_group, u.category
  FROM all_groups g CROSS JOIN (
    SELECT 'Hovedstaden' AS category UNION ALL
    SELECT 'Sjælland' UNION ALL
    SELECT 'Syddanmark' UNION ALL
    SELECT 'Midtjylland' UNION ALL
    SELECT 'Nordjylland' UNION ALL
    SELECT 'others'
  ) u
),
pct_subscriber_zip AS (
  SELECT
    tc.engagement_group,
    tc.category,
    ROUND(
      CASE WHEN tt.total = 0 OR tt.total IS NULL THEN 0
           ELSE 100 * COALESCE(tb.cnt, 0) / tt.total END, 2
    ) AS pct
  FROM subscriber_zip_cross tc
  LEFT JOIN subscriber_zip tb
    ON tb.engagement_group = tc.engagement_group AND tb.category = tc.category
  LEFT JOIN subscriber_zip_totals tt
    ON tt.engagement_group = tc.engagement_group
),
-- 8️⃣ Series and categories
series_subscriber_group AS (
  SELECT CONCAT(
    '[',
    GROUP_CONCAT(
      CONCAT('{"name":"', engagement_group, '","data":[', data_list, ']}')
      ORDER BY FIELD(engagement_group,'Ingen','Lav','Middel','Høj') SEPARATOR ','
    ),
    ']'
  ) AS series_json
  FROM (
    SELECT engagement_group,
           GROUP_CONCAT(pct ORDER BY FIELD(category,'Ukendt gruppe','Others') SEPARATOR ',') AS data_list
    FROM pct_subscriber_group
    GROUP BY engagement_group
  ) t
),


series_subscriber_sindri AS (
  SELECT CONCAT(
    '[',
    GROUP_CONCAT(
      CONCAT('{"name":"', engagement_group, '","data":[', data_list, ']}')
      ORDER BY FIELD(engagement_group,'Ingen','Lav','Middel','Høj') SEPARATOR ','
    ),
    ']'
  ) AS series_json
  FROM (
    SELECT engagement_group,
           GROUP_CONCAT(pct ORDER BY FIELD(category,'<47','48-60','>60','others') SEPARATOR ',') AS data_list
    FROM pct_subscriber_sindri
    GROUP BY engagement_group
  ) t
),

series_subscriber_gender AS (
  SELECT CONCAT(
    '[',
    GROUP_CONCAT(
      CONCAT('{"name":"', engagement_group, '","data":[', data_list, ']}')
      ORDER BY FIELD(engagement_group,'Ingen','Lav','Middel','Høj') SEPARATOR ','
    ),
    ']'
  ) AS series_json
  FROM (
    SELECT engagement_group,
           GROUP_CONCAT(pct ORDER BY FIELD(category,'Mand','Kvinde','others') SEPARATOR ',') AS data_list
    FROM pct_subscriber_gender
    GROUP BY engagement_group
  ) t
),

series_subscriber_zip AS (
  SELECT CONCAT(
    '[',
    GROUP_CONCAT(
      CONCAT('{"name":"', engagement_group, '","data":[', data_list, ']}')
      ORDER BY FIELD(engagement_group,'Ingen','Lav','Middel','Høj') SEPARATOR ','
    ),
    ']'
  ) AS series_json
  FROM (
    SELECT engagement_group,
           GROUP_CONCAT(pct ORDER BY FIELD(category,'Hovedstaden','Sjælland','Syddanmark','Midtjylland','Nordjylland','others') SEPARATOR ',') AS data_list
    FROM pct_subscriber_zip
    GROUP BY engagement_group
  ) t
),

series_subscriber_position_of_trust AS (
  SELECT CONCAT(
    '[',
    GROUP_CONCAT(
      CONCAT('{"name":"', engagement_group, '","data":[', data_list, ']}')
      ORDER BY FIELD(engagement_group,'Ingen','Lav','Middel','Høj') SEPARATOR ','
    ),
    ']'
  ) AS series_json
  FROM (
    SELECT engagement_group,
           GROUP_CONCAT(pct ORDER BY FIELD(category,'Nej','Ja','others') SEPARATOR ',') AS data_list
    FROM pct_subscriber_position_of_trust
    GROUP BY engagement_group
  ) t
),



cats_subscriber_group AS (SELECT '["Ukendt gruppe","others"]' AS cats),
cats_subscriber_sindri AS (SELECT '["<47","48-60",">60","others"]' AS cats),
cats_subscriber_gender AS (SELECT '["Mand","Kvinde","others"]' AS cats),
cats_subscriber_zip AS (SELECT '["Hovedstaden","Sjælland","Syddanmark","Midtjylland","Nordjylland","others"]' AS cats),
cats_subscriber_position_of_trust AS (SELECT '["Nej","Ja","others"]' AS cats)

-- Categories


-- 9️⃣ Final JSON output
SELECT CONCAT(
  '{',
    '"site":', 14, ',',
    '"data":{',
      '"defaultTitle":"Gruppe",',
      '"categories":', (SELECT cats FROM cats_subscriber_group), ',',
      '"label":"Modtagere ift. engagementsgruppe",',
      '"series":', (SELECT series_json FROM series_subscriber_group), ',',
      '"additional":[',
        '{',
          '"title":"Tillidshverv",',
          '"data":{',
            '"label":"Modtagere ift. engagementsgruppe",',
            '"categories":', (SELECT cats FROM cats_subscriber_position_of_trust), ',',
            '"series":', (SELECT series_json FROM series_subscriber_position_of_trust),
          '}',
        '},',
        '{',
          '"title":"Køn",',
          '"data":{',
            '"label":"Modtagere ift. engagementsgruppe",',
            '"categories":', (SELECT cats FROM cats_subscriber_gender), ',',
            '"series":', (SELECT series_json FROM series_subscriber_gender),
          '}',
        '},',
        '{',
          '"title":"Alder",',
          '"data":{',
            '"label":"Modtagere ift. engagementsgruppe",',
            '"categories":', (SELECT cats FROM cats_subscriber_sindri), ',',
            '"series":', (SELECT series_json FROM series_subscriber_sindri),
          '}',
        '},',
        '{',
          '"title":"Geografi",',
          '"data":{',
            '"label":"Modtagere ift. engagementsgruppe",',
            '"categories":', (SELECT cats FROM cats_subscriber_zip), ',',
            '"series":', (SELECT series_json FROM series_subscriber_zip),
          '}',
        '}',
      ']',
    '}',
  '}'
) AS json_result;
