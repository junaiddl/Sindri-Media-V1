WITH ranked AS (
    -- Total row first
    SELECT siteid, 'Total' AS name, 0 AS rn
    FROM pre_stage.click_events
    WHERE siteid = 14
    GROUP BY siteid

    UNION ALL

    -- Mailing list rows
    SELECT siteid, mailing_name AS name,
           ROW_NUMBER() OVER (PARTITION BY siteid ORDER BY mailing_list) AS rn
    FROM (
        SELECT DISTINCT siteid, mailing_list, mailing_name
        FROM pre_stage.click_events
        WHERE siteid = 14
    ) AS t
)
SELECT JSON_OBJECT(
    'site', siteid,
    'data', JSON_ARRAYAGG(
        JSON_OBJECT(
            'code', CONCAT(siteid, '.', rn),
            'name', name
        )
    )
) AS site_json
FROM ranked
GROUP BY siteid;
