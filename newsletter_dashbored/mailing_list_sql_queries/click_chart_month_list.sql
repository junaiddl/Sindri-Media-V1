WITH master_list AS (
    SELECT
        site_id AS siteid,
        subscriber_token
    FROM pre_stage.whoissubscribed_data
    WHERE DATE(created_at) = CURDATE() - INTERVAL 2 DAY
    and list_id = 1
),

click_counts AS (
    SELECT
        siteid,
        subscriber_token,
        COUNT(*) AS clicks
    FROM pre_stage.click_events
	where created_date BETWEEN DATE_SUB(NOW(), INTERVAL 31 DAY) AND DATE_SUB(NOW(), INTERVAL 1 DAY)
    and mailing_list = 1
    GROUP BY siteid, subscriber_token
),

subscriber_clicks AS (
    SELECT
        m.siteid,
        m.subscriber_token,
        COALESCE(c.clicks, 0) AS clicks
    FROM master_list m
    LEFT JOIN click_counts c
        ON m.siteid = c.siteid
       AND m.subscriber_token = c.subscriber_token
),

ranked AS (
    SELECT
        *,
        PERCENT_RANK() OVER (
            PARTITION BY siteid
            ORDER BY clicks DESC
        ) AS pr
    FROM subscriber_clicks
),

engagement_groups AS (
    SELECT
        siteid,
        subscriber_token,
        clicks,
        CASE
            WHEN clicks = 0 THEN 'Ingen'
            WHEN clicks BETWEEN 1 AND 2 THEN 'Lav'
            WHEN pr <= 0.10 THEN 'Høj'
            WHEN clicks >= 3 AND pr > 0.10 THEN 'Mellem'
        END AS engagement_group
    FROM ranked
),

agg AS (
    SELECT
        siteid,
        engagement_group,
        COUNT(*) AS subscribers,
        SUM(clicks) AS clicks
    FROM engagement_groups
    GROUP BY siteid, engagement_group
),

totals AS (
    SELECT
        siteid,
        SUM(subscribers) AS total_subscribers,
        SUM(clicks) AS total_clicks
    FROM agg
    GROUP BY siteid
),

percentages AS (
    SELECT
        a.siteid,
        a.engagement_group,
        ROUND(100 * a.subscribers / t.total_subscribers, 2) AS andel_modtagere,
        ROUND(100 * a.clicks / NULLIF(t.total_clicks, 0), 2) AS andel_kliks
    FROM agg a
    JOIN totals t
      ON a.siteid = t.siteid
)

SELECT
    JSON_OBJECT(
        'site', siteid,
        'data', JSON_OBJECT(
            'label', 'Kliks ift engagementsgruppe',
            'categories', JSON_ARRAY(
                'Ingen',
                'Lav',
                'Mellem',
                'Høj'
            ),
            'series', JSON_ARRAY(
                JSON_OBJECT(
                    'name', 'Andel modtagere',
                    'data', JSON_ARRAY(
                        MAX(CASE WHEN engagement_group = 'Ingen' THEN andel_modtagere END),
                        MAX(CASE WHEN engagement_group = 'Lav' THEN andel_modtagere END),
                        MAX(CASE WHEN engagement_group = 'Mellem' THEN andel_modtagere END),
                        MAX(CASE WHEN engagement_group = 'Høj' THEN andel_modtagere END)
                    )
                ),
                JSON_OBJECT(
                    'name', 'Andel kliks',
                    'data', JSON_ARRAY(
                        MAX(CASE WHEN engagement_group = 'Ingen' THEN andel_kliks END),
                        MAX(CASE WHEN engagement_group = 'Lav' THEN andel_kliks END),
                        MAX(CASE WHEN engagement_group = 'Mellem' THEN andel_kliks END),
                        MAX(CASE WHEN engagement_group = 'Høj' THEN andel_kliks END)
                    )
                )
            )
        )
    ) AS result
FROM percentages
GROUP BY siteid;
