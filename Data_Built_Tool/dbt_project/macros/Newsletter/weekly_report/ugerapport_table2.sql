{% macro ugerapport_table2_dbt_newsletter() %}

{% set site = var('site') %}
{% set weekly_referrers = var('Traffic').weekly_report_traffic_channel %}


CREATE PROCEDURE `ugerapport_table2_dbt_{{site}}`()
BEGIN


WITH
-- Known realreferrers
known_referrers AS (
  SELECT DISTINCT realreferrer
  FROM pre_stage.ref_value
  WHERE siteid = {{site}}
),

-- Helper for referrer mapping

traffic_mapped AS (
  SELECT
    CASE
      WHEN tc.realreferrer IN ({{ weekly_referrers }}) THEN tc.realreferrer
      WHEN tc.realreferrer LIKE 'NL%' THEN 'Newsletter'
      ELSE 'Others'
    END AS mapped_referrer,
    tc.date
  FROM prod.traffic_channels tc
  WHERE tc.siteid = {{site}}
),


-- traffic_mapped AS (
--   SELECT
--     CASE
--       WHEN tc.realreferrer IN ('Direct', 'Search', 'Facebook') THEN tc.realreferrer
--       WHEN tc.realreferrer = 'Newsletter' OR tc.realreferrer LIKE 'NL%' THEN 'Newsletter'
--       ELSE 'Others'
--     END AS mapped_referrer,

--     tc.date
--   FROM prod.traffic_channels tc
--   WHERE tc.siteid = {{site}}
-- ),


-- Define week ranges
week_bounds AS (
  SELECT
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 7) DAY) AS last_monday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 1) DAY) AS last_sunday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 14) DAY) AS prev_monday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 8) DAY) AS prev_sunday
),


-- Total count of last week's visits across all referrers
total_counts AS (
  SELECT SUM(weekly_count) AS total_count
  FROM (
    SELECT mapped_referrer, COUNT(*) AS weekly_count
    FROM traffic_mapped, week_bounds
    WHERE date BETWEEN last_monday AND last_sunday
    GROUP BY mapped_referrer
  ) AS sub
),



-- Last week counts
last_week_counts AS (
  SELECT mapped_referrer AS realreferrer, COUNT(*) AS weekly_count
  FROM traffic_mapped, week_bounds
  WHERE date BETWEEN last_monday AND last_sunday
  GROUP BY mapped_referrer
),

-- Previous week counts
previous_week_counts AS (
  SELECT mapped_referrer AS realreferrer, COUNT(*) AS prev_weekly_count
  FROM traffic_mapped, week_bounds
  WHERE date BETWEEN prev_monday AND prev_sunday
  GROUP BY mapped_referrer
),


-- Combine all data
combined AS (
  SELECT 
    l.realreferrer,
    tc.total_count,
    COALESCE(l.weekly_count, 0) AS weekly_count,
    COALESCE(p.prev_weekly_count, 0) AS prev_weekly_count
  FROM last_week_counts l
  LEFT JOIN previous_week_counts p ON l.realreferrer = p.realreferrer
  CROSS JOIN total_counts tc
)


-- Final JSON output
SELECT JSON_OBJECT(
  'site', {{site}},
  'data', JSON_ARRAYAGG(
    JSON_OBJECT(
      'kanal', realreferrer,
      'besog_uge', weekly_count,
      'andel_af_total', CONCAT(
        ROUND(
          CASE 
            WHEN total_count = 0 THEN 0
            ELSE (weekly_count / total_count) * 100
          END, 
          1
        ), '%'
      ),
      'udvikling_ift_forrige_uge', CONCAT(
        CASE 
          WHEN prev_weekly_count = 0 THEN '0.0'
          WHEN (weekly_count - prev_weekly_count) / prev_weekly_count >= 0 THEN '+'
          ELSE '-'
        END,
        ROUND(
          CASE 
            WHEN prev_weekly_count = 0 THEN 0
            ELSE ABS((weekly_count - prev_weekly_count) / prev_weekly_count) * 100
          END,
          1
        ), '%'
      )
    )
  )
) AS json_result
FROM combined;


END
{% endmacro %}