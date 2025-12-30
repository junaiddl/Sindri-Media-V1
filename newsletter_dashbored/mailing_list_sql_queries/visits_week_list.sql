SET SESSION group_concat_max_len = 100000;

WITH date_range AS (
    SELECT 
        DATE_SUB(MAX(created_at), INTERVAL 13 MONTH) AS start_date,
        MAX(created_at) AS end_date
    FROM pre_stage.total_subscriber_stats
),

weekly_data AS (
    SELECT
        s.site_id,
        WEEK(s.created_at, 3) AS week_num,
        Sum(COALESCE(s.subscribed_count, 0)) AS new_subscribers,
        Sum(COALESCE(s.unsubscribed_count, 0)) AS unsubscribers,
        AVG(COALESCE(s.subscriber_count, 0)) AS goal_subscribers
    FROM pre_stage.total_subscriber_stats s
    JOIN date_range r
      ON s.created_at BETWEEN r.start_date AND r.end_date
    WHERE s.site_id = 14 and s.list_id=1
    GROUP BY s.site_id, WEEK(s.created_at, 3)
    
),

ordered AS (
    SELECT 
        site_id,
        week_num,
        new_subscribers,
        unsubscribers,
        goal_subscribers
    FROM weekly_data
    ORDER BY site_id, week_num
)

SELECT 
    CONCAT(
        '{',
        '"site":', site_id, ',',
        '"data":{',
            '"label":"Subscribers",',
            '"hint":"Udvikling i subscribers hhv. dag, uge og måned",',
            '"bars":["True","True","False"],',
            '"categories":[', GROUP_CONCAT(week_num ORDER BY week_num), '],',
            '"data":[', GROUP_CONCAT(new_subscribers ORDER BY week_num), '],',
            '"data_prev":[', GROUP_CONCAT(unsubscribers ORDER BY week_num), '],',
            '"goals":[', GROUP_CONCAT(goal_subscribers ORDER BY week_num), ']',
        '}',
        '}'
    ) AS output_json
FROM ordered
GROUP BY site_id;
