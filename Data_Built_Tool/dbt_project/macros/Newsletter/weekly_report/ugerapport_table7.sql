{% macro ugerapport_table7_dbt_newsletter() %}

{% set site = var('site') %}
{% set event_name = var('event_action') %}

CREATE PROCEDURE `ugerapport_table7_dbt_{{site}}`()
BEGIN

WITH date_bounds AS (
  SELECT
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) AS week_start,
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AS week_end
),

-- Include all articles (no filter on publication date)
all_articles AS (
  SELECT id AS postid, title, date, userneeds
  FROM prod.site_archive_post
  WHERE siteid = {{site}}
),

-- Only pageviews from last week
pageviews_per_article AS (
  SELECT postid, SUM(unique_pageviews) AS total_pageviews
  FROM prod.pages, date_bounds
  WHERE siteid = {{site}}
    AND date BETWEEN week_start AND week_end
  GROUP BY postid
),

-- Only CTA hits (Next Clicks) from last week
cta_per_article AS (
  SELECT postid, SUM(hits) AS total_cta
  FROM prod.events, date_bounds
  WHERE siteid = {{site}}
    AND event_action = '{{event_name}}'
    AND date BETWEEN week_start AND week_end
  GROUP BY postid
),

-- Join all together
article_metrics AS (
  SELECT
    a.title,
    a.date,
    a.userneeds AS userneed,
    COALESCE(p.total_pageviews, 0) AS unique_pageviews,
    ROUND(COALESCE(e.total_cta, 0), 2) AS gns_cta
  FROM all_articles a
  LEFT JOIN pageviews_per_article p ON a.postid = p.postid
  LEFT JOIN cta_per_article e ON a.postid = e.postid
)

-- Final JSON output
SELECT JSON_OBJECT(
  'site', {{site}},
  'data', JSON_ARRAYAGG(
    JSON_OBJECT(
      'titel', title,
      'dato', date,
      'brugerbehov', userneed,
      'sidevisninger', unique_pageviews,
      'gns_cta', gns_cta
    )
  )
) AS table7
FROM (
  SELECT *
  FROM article_metrics
  ORDER BY unique_pageviews DESC
  LIMIT 3
) AS top_articles;

END

{% endmacro %}
