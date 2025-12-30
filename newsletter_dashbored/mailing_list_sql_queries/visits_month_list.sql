SET SESSION group_concat_max_len = 100000;

WITH date_range AS (
    SELECT 
        DATE_SUB(MAX(created_at), INTERVAL 13 MONTH) AS start_date,
        MAX(created_at) AS end_date
    FROM pre_stage.total_subscriber_stats
),

monthly_data AS (
    SELECT
        s.site_id,
        MONTH(s.created_at) AS month,
        DATE_FORMAT(s.created_at, '%b') AS monthname,
        sum(COALESCE(s.subscribed_count, 0)) AS new_subscribers,
        sum(COALESCE(s.unsubscribed_count, 0)) AS unsubscribers,
        AVG(COALESCE(s.subscriber_count, 0)) AS goal_subscribers
    FROM pre_stage.total_subscriber_stats s
    JOIN date_range r
      ON s.created_at BETWEEN r.start_date AND r.end_date
    WHERE s.site_id = 14 and s.list_id=1
    GROUP BY s.site_id, MONTH(s.created_at), DATE_FORMAT(s.created_at, '%b')
),

ordered AS (
    SELECT 
        site_id,
        month,
        monthname,
        new_subscribers,
        unsubscribers,
        goal_subscribers
    FROM monthly_data
    ORDER BY site_id, month
)

SELECT 
    CONCAT(
        '{',
        '"site":', site_id, ',',
        '"data":{',
            '"label":"Subscribers",',
            '"hint":"Udvikling i subscribers hhv. dag, uge og måned",',
            '"bars":["True","True","False"],',
            '"categories":[', GROUP_CONCAT(CONCAT('"', monthname, '"') ORDER BY month), '],',
            '"data":[', GROUP_CONCAT(new_subscribers ORDER BY month), '],',
            '"data_prev":[', GROUP_CONCAT(unsubscribers ORDER BY month), '],',
            '"goals":[', GROUP_CONCAT(goal_subscribers ORDER BY month), ']',
        '}',
        '}'
    ) AS output_json
FROM ordered
GROUP BY site_id;
