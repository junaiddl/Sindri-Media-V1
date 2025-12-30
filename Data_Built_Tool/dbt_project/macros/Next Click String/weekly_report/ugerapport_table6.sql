{% macro string_ugerapport_table6_dbt_next_click() %}

{% set site = var('site') %}
{% set event_name = var('event_action')%}

CREATE PROCEDURE `ugerapport_table6_dbt_{{site}}`()
BEGIN

WITH

-- Date logic for last week's Monday and Sunday
date_bounds AS (
  SELECT
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 7 DAY) AS last_monday,
    DATE_SUB(CURDATE(), INTERVAL WEEKDAY(CURDATE()) + 1 DAY) AS last_sunday
),

-- Get articles published last week
last_week_articles AS (
  SELECT id AS postid, title, date, userneeds
  FROM prod.site_archive_post, date_bounds
  WHERE siteid = {{site}}
    AND date BETWEEN last_monday AND last_sunday
),

-- Get total unique pageviews per article
pageviews_per_article AS (
  SELECT postid, SUM(unique_pageviews) AS total_pageviews
  FROM prod.pages, date_bounds
  WHERE siteid = {{site}}
    AND date BETWEEN last_monday AND last_sunday
  GROUP BY postid
),

-- Get total CTA (Next Clicks) per article
cta_per_article AS (
  SELECT postid, SUM(hits) AS total_cta
  FROM prod.events, date_bounds
  WHERE siteid = {{site}}
    AND event_action = '{{event_name}}'
    AND date BETWEEN last_monday AND last_sunday
  GROUP BY postid
),

-- Join articles with metrics
article_metrics AS (
  SELECT
    a.title,
    a.date,
    a.userneeds AS userneed,
    COALESCE(p.total_pageviews, 0) AS unique_pageviews,
    ROUND(COALESCE(e.total_cta, 0) / NULLIF(p.total_pageviews, 0) * 100, 2) AS gns_cta
  FROM last_week_articles a
  LEFT JOIN pageviews_per_article p ON a.postid = p.postid
  LEFT JOIN cta_per_article e ON a.postid = e.postid
)

-- Final JSON output for Table6
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
) AS table6
FROM (
  SELECT *
  FROM article_metrics
  ORDER BY unique_pageviews DESC
  LIMIT 3
) AS top_articles;

END
{% endmacro %}