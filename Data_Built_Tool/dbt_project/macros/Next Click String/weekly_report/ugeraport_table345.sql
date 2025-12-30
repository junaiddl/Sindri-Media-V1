{% macro string_ugerapport_table345_dbt_next_click() %}

{% set site = var('site') %}
{% set event_name = var('event_action') %}
{% set cta_goal = 8 %}
{% set pageview_goal = 1000 %}

CREATE PROCEDURE `ugerapport_table345_dbt_{{site}}`()
BEGIN

WITH 

week_bounds AS (
  SELECT
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 1) DAY) AS last_sunday,
    DATE_SUB(CURDATE(), INTERVAL (WEEKDAY(CURDATE()) + 7) DAY) AS last_monday
),


last_week_articles AS (
    SELECT id AS postid, title, date, userneeds
    FROM prod.site_archive_post, week_bounds wb
    WHERE siteid = {{site}} 
      AND date BETWEEN wb.last_monday AND wb.last_sunday

),

pageviews AS (
    SELECT postid, SUM(unique_pageviews) AS total_pageviews
    FROM prod.pages, week_bounds wb
    WHERE siteid = {{site}} 
      AND date BETWEEN wb.last_monday AND wb.last_sunday

    GROUP BY postid
),

cta AS (
    SELECT postid, SUM(hits) AS total_cta
    FROM prod.events, week_bounds wb
    WHERE siteid = {{site}} 
      AND event_action = '{{event_name}}'
      AND WHERE date BETWEEN wb.last_monday AND wb.last_sunday
    GROUP BY postid
),

combined AS (
    SELECT 
        a.postid,
        a.userneeds,
        COALESCE(p.total_pageviews, 0) AS pageviews,
        COALESCE(c.total_cta, 0) AS cta,
        ROUND(COALESCE(c.total_cta, 0) / NULLIF(p.total_pageviews, 0) * 100, 2) AS gns_cta
    FROM last_week_articles a
    LEFT JOIN pageviews p ON a.postid = p.postid
    LEFT JOIN cta c ON a.postid = c.postid
),


summary AS (
    SELECT 
        ROUND(AVG(gns_cta), 2) AS gns_cta,
        COUNT(*) AS antal_artikler,
        ROUND(AVG(pageviews), 0) AS gns_sidevisninger
    FROM combined
),

goals AS (
    SELECT
        min_cta,
        min_pageviews
    FROM prod.goals
    WHERE site_id = 4
    AND WEEK = WEEK(CURDATE(), 3) - 1 AND
    YEAR(date) = YEAR(CURDATE())
    LIMIT 1
),

table4 AS (
    SELECT
        ROUND(SUM(CASE WHEN c.gns_cta >= (SELECT min_cta FROM goals) THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) AS over_et_maal,
        ROUND(SUM(CASE WHEN c.gns_cta >= (SELECT min_cta FROM goals) AND c.pageviews >= (SELECT min_pageviews FROM goals) THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) AS over_begge_maal,
        ROUND(SUM(CASE WHEN c.gns_cta < (SELECT min_cta FROM goals) AND c.pageviews < (SELECT min_pageviews FROM goals) THEN 1 ELSE 0 END) / COUNT(*) * 100, 1) AS under_begge_maal
    FROM combined c
),

table5 AS (
    SELECT 
        userneeds AS brugerbehov,
        ROUND(AVG(gns_cta), 2) AS gns_cta,
        ROUND(AVG(pageviews), 0) AS gns_sidevisninger,
        ROUND(COUNT(*) / (SELECT COUNT(*) FROM combined) * 100, 1) AS andel
    FROM combined
    GROUP BY userneeds
)

SELECT JSON_OBJECT(
    'site', {{site}},
    'data', JSON_OBJECT(
        'table3', (
            SELECT JSON_OBJECT(
                'gns_cta', s.gns_cta,
                'antal_artikler', s.antal_artikler,
                'gns_sidevisninger', s.gns_sidevisninger
            )
            FROM summary s
        ),
        'table4', (
            SELECT JSON_OBJECT(
                'over_et_maal', t.over_et_maal,
                'over_begge_maal', t.over_begge_maal,
                'under_begge_maal', t.under_begge_maal
            )
            FROM table4 t
        ),
        'table5', (
            SELECT JSON_ARRAYAGG(
                JSON_OBJECT(
                    'brugerbehov', t5.brugerbehov,
                    'gns_cta', t5.gns_cta,
                    'gns_sidevisninger', t5.gns_sidevisninger,
                    'andel', t5.andel
                )
            )
            FROM table5 t5
        )
    )
) AS table345_output;

END;

{% endmacro %}
