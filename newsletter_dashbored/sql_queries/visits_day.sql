SET SESSION group_concat_max_len = 100000;

WITH date_range AS (
    SELECT 
        DATE_SUB(MAX(date), INTERVAL 13 MONTH) AS start_date,
        MAX(date) AS end_date
    FROM pre_stage.subscriber_master_list
),
daily_agg AS (
    SELECT
        s.site_id,
        DATE(s.date) AS period_start,
        SUM(COALESCE(s.subscribed_count, 0)) AS new_subscribers,
        SUM(COALESCE(s.unsubscribed_count, 0)) AS unsubscribers,
        MAX(s.subscriber_count) AS goal_subscribers
    FROM pre_stage.subscriber_master_list s
    JOIN date_range r
      ON s.date BETWEEN r.start_date AND r.end_date
    
    GROUP BY s.site_id, DATE(s.date)
)
SELECT 
    JSON_OBJECT(
        'site', site_id,
        'data', JSON_OBJECT(
            'hint', 'Udvikling i subscribers hhv. dag, uge og måned',
            'bars', JSON_ARRAY(TRUE, TRUE, FALSE),
            'label', 'Subscribers',
            'categories', JSON_ARRAYAGG(DATE_FORMAT(period_start, '%d/%m')),
            'data', JSON_ARRAYAGG(new_subscribers),
            'data_prev', JSON_ARRAYAGG(unsubscribers),
            'goals', JSON_ARRAYAGG(goal_subscribers)
        )
    ) AS output_json
FROM (
    SELECT *
    FROM daily_agg
    WHERE site_id = 14               -- still optional/hardcoded filter
    ORDER BY period_start
) ordered
GROUP BY site_id;
