{% macro next_click_bigview_header_week_dbt() %}
    {% set siteid = var('site')%}
    {% set event_action = var('event_action')%}

CREATE PROCEDURE `bigview_header_week_dbt_{{siteid}}`()
BEGIN
WITH unique_pageviews_yesterday AS (
    SELECT
        SiteID,
        SUM(Unique_pageviews) AS value
    FROM
        prod.daily_totals
    WHERE
        date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
        AND siteid = {{siteid}}
    GROUP BY SiteId
),

unique_pageviews_day_before_yesterday AS (
    SELECT
        SiteID,
        SUM(Unique_pageviews) AS value_day_before
    FROM
        prod.daily_totals
    WHERE
        date BETWEEN DATE_SUB(NOW(), INTERVAL 15 DAY) AND DATE_SUB(NOW(), INTERVAL 8 DAY)
        AND siteid = {{siteid}}
    GROUP BY SiteId
),


unique_pageviews_card AS (
    SELECT {{siteid}} as siteid, y.value, b.value_day_before
    FROM unique_pageviews_yesterday y
    JOIN unique_pageviews_day_before_yesterday b
    ON y.siteid = b.siteid
),

visits_yesterday as (
    SELECT
    SiteID,sum(Visits) as value
    FROM
        prod.daily_totals
    WHERE
    date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
    AND siteid = {{siteid}}
    group by SiteId
    ),

visits_day_before_yesterday as (
    SELECT
    SiteID,sum(Visits) as value_day_before
    FROM
        prod.daily_totals
    WHERE
    date BETWEEN DATE_SUB(NOW(), INTERVAL 15 DAY) AND DATE_SUB(NOW(), INTERVAL 8 DAY)
    AND siteid = {{siteid}}
    group by SiteId
    ),

visits_card AS(
    SELECT {{siteid}} as siteid, y.value, b.value_day_before
    FROM visits_yesterday y
    JOIN visits_day_before_yesterday b
    ON y.siteid = b.siteid
),

events_yesterday as (
    SELECT
        e.SiteID as SiteID,
        e.date as date,
        SUM(e.hits) AS event_value
    FROM
        prod.events e
    WHERE
    e.siteid = {{siteid}} and
        e.date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
        AND e.event_action = '{{event_action}}'
    GROUP BY SiteId,2
    ),

events_day_before as (
    SELECT
    e.SiteID as SiteID,
    e.date as date,
    SUM(e.hits) as event_value_day_before
    FROM
        prod.events e
    WHERE
    e.date BETWEEN DATE_SUB(NOW(), INTERVAL 15 DAY) AND DATE_SUB(NOW(), INTERVAL 8 DAY)
    and e.siteid = {{siteid}}
    AND e.event_action = '{{event_action}}'
    group by SiteId,2
    ),

pages_yesterday as (
    SELECT
        p.SiteID as SiteID,
        p.date,
        SUM(p.unique_pageviews) AS page_value
    FROM
        prod.pages p
    WHERE
        p.siteid = {{siteid}} and
        p.date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
    GROUP BY SiteId,2
    ),
pages_day_before as (
    SELECT
        p.SiteID as SiteID,
        p.date,
        SUM(p.unique_pageviews)  as page_value_day_before
    FROM
        prod.pages p
    WHERE
    p.date BETWEEN DATE_SUB(NOW(), INTERVAL 15 DAY) AND DATE_SUB(NOW(), INTERVAL 8 DAY)
    and p.siteid = {{siteid}}
    group by SiteId,2
    ),

combined_ratios_yesterday AS (
    SELECT
        e.SiteID,
        e.date,
        e.event_value / NULLIF(p.page_value, 0) AS ratio_yesterday
    FROM events_yesterday e
    LEFT JOIN pages_yesterday p ON e.SiteID = p.SiteID AND e.date = p.date
    ),

combined_ratios_before AS (
    SELECT
        e_day_before.SiteID,
        DATE_ADD(e_day_before.date, INTERVAL 7 DAY) AS date,
        e_day_before.event_value_day_before / NULLIF(p_day_before.page_value_day_before, 0) AS ratio_day_before

    FROM events_day_before e_day_before
    LEFT JOIN pages_day_before p_day_before ON e_day_before.SiteID = p_day_before.SiteID AND e_day_before.date = p_day_before.date
    ),

combined_ratios AS(
    SELECT crb.SiteID AS siteid, ROUND((SUM(cry.ratio_yesterday)/count(*) * 100),1) as ratio_yesterday, ROUND((SUM(crb.ratio_day_before)/count(*)) * 100 ,1) as ratio_day_before
    FROM combined_ratios_before crb
    LEFT JOIN combined_ratios_yesterday cry ON cry.SiteID = crb.SiteID AND crb.date = cry.date
    GROUP BY 1
),

next_click_card as (
    select  siteid, ratio_yesterday as yr_final, ratio_day_before as rd_final
    from combined_ratios
),

unique_pageviews_chart as (
    SELECT SiteID,date,SUM(Unique_pageviews) as data
    FROM prod.daily_totals
    where date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY) and siteid = {{siteid}}
    group by 1,2
    ),

unique_pageviews_chart_json AS (
    SELECT 
    {{siteid}} as siteid, 
    GROUP_CONCAT( CONCAT(upc.data) order by upc.date  SEPARATOR ', ') AS json_data
    FROM unique_pageviews_chart upc
),

visits_chart as (
    SELECT SiteID, date,SUM(Visits) as data
    FROM prod.daily_totals
    where
    date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY) and siteid = {{siteid}}
    group by 1,2
    ),

visits_chart_json AS (
    SELECT 
    {{siteid}} as siteid, 
    GROUP_CONCAT( CONCAT(vc.data) order by vc.date  SEPARATOR ', ') AS json_data
    FROM visits_chart vc
),

events_chart AS (
    SELECT
        e.SiteID as SiteID,
        e.date as date,
        SUM(e.HITS) AS data
    FROM
        prod.events e
    WHERE
        e.date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
        AND e.event_action = '{{event_action}}'
        AND e.siteid = {{siteid}}
    GROUP BY 1, 2
    ),

pages_chart as(
    SELECT
        p.siteid as siteid,
        p.date as date,
        COALESCE(SUM(p.unique_pageviews), 0) AS sum_of_pageviews
    FROM
        prod.pages p
    WHERE
        p.date BETWEEN DATE_SUB(NOW(), INTERVAL 8 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
        and p.siteid = {{siteid}}
    GROUP BY 1,2
    ),

next_click_chart AS(
    SELECT {{siteid}} AS siteid, e.date AS date, ROUND((SUM(e.data)/SUM(p.sum_of_pageviews))*100,1) AS next_click_data
    FROM events_chart e
    LEFT JOIN pages_chart p ON e.siteid = p.siteid AND e.date = p.date
    GROUP BY 1,2
    ORDER BY e.date ASC
),

next_click_chart_json AS (
    SELECT 
    {{siteid}} as siteid, 
    GROUP_CONCAT( CONCAT(ncc.next_click_data) order by ncc.date SEPARATOR ', ') AS json_data
    FROM next_click_chart ncc
)


SELECT
    CONCAT('{
                "site": {{siteid}},
                "data": {
                    "cards": [
                    {
                        "chart": {
                        "data": [
                            ['
                            ,IFNULL(upch.json_data, 0),
                            ']
                        ],
                        "series": [
                            "Series 1"
                        ]
                        },
                        "title": "Sidevisninger sidste 7 dage",
                        "value": "',IFNULL(upc.value, 0),'",
                        "change": "',IFNULL(upc.value_day_before, 0),'"
                        },
                        {
                            "chart": {
                            "data": [
                                [', IFNULL(vch.json_data, 0),']
                        ],
                        "series": [
                            "Series 1"
                        ]
                        },
                        "title": "Besøg sidste 7 dage",
                        "value": "',IFNULL(vc.value, 0),'",
                        "change": "',IFNULL(vc.value_day_before, 0),'"
                        },
                        {
                            "chart": {
                            "data": [
                                [',IFNULL(ncch.json_data, 0),']
                        ],
                        "series": [
                            "Series 1"
                        ]
                        },
                        "title": "Next click sidste 7 dage",
                        "value": "',IFNULL(ncc.yr_final, 0),'",
                        "change": "',IFNULL(ncc.rd_final, 0),'"
                        }
                        ]
                    }
                    }') AS json_data
FROM unique_pageviews_card upc
LEFT JOIN visits_card vc ON upc.siteid = vc.siteid
LEFT JOIN next_click_card ncc ON upc.siteid = ncc.siteid
LEFT JOIN visits_chart_json vch ON upc.siteid = vch.siteid
LEFT JOIN unique_pageviews_chart_json upch ON upc.siteid = upch.siteid
LEFT JOIN next_click_chart_json ncch ON upc.siteid = ncch.siteid;


END

{% endmacro %}