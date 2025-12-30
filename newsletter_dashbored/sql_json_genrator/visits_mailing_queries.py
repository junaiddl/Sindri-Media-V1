

def visits_mailing_queries(list_id):
    visits_day_query = f""" 
   

WITH date_range AS (
    SELECT 
        DATE_SUB(MAX(created_at), INTERVAL 13 MONTH) AS start_date,
        MAX(created_at) AS end_date
    FROM pre_stage.total_subscriber_stats
),
daily_agg AS (
    SELECT
        s.site_id,
        DATE(s.created_at) AS period_start,
        SUM(COALESCE(s.subscribed_count, 0)) AS new_subscribers,
        SUM(COALESCE(s.unsubscribed_count, 0)) AS unsubscribers,
        MAX(s.subscriber_count) AS goal_subscribers
    FROM pre_stage.total_subscriber_stats s
    JOIN date_range r
      ON s.created_at BETWEEN r.start_date AND r.end_date
      where s.list_id={list_id}
    GROUP BY s.site_id, DATE(s.created_at)
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

    """

    visits_ytd_query = f"""
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
    WHERE s.site_id = 14 and s.list_id= {list_id}
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
        '{{',
        '"site":', site_id, ',',
        '"data":{{',
            '"label":"Subscribers",',
            '"hint":"Udvikling i subscribers hhv. dag, uge og måned",',
            '"bars":[true,true,false],',
            '"categories":[', GROUP_CONCAT(CONCAT('"', monthname, '"') ORDER BY month), '],',
            '"data":[', GROUP_CONCAT(new_subscribers ORDER BY month), '],',
            '"data_prev":[', GROUP_CONCAT(unsubscribers ORDER BY month), '],',
            '"goals":[', GROUP_CONCAT(goal_subscribers ORDER BY month), ']',
        '}}',
        '}}'
    ) AS output_json
FROM ordered
GROUP BY site_id;
    """

    visits_month_query = f"""


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
        '{{',
        '"site":', site_id, ',',
        '"data":{{',
            '"label":"Subscribers",',
            '"hint":"Udvikling i subscribers hhv. dag, uge og måned",',
            '"bars":[true,true,false],',
            '"categories":[', GROUP_CONCAT(week_num ORDER BY week_num), '],',
            '"data":[', GROUP_CONCAT(new_subscribers ORDER BY week_num), '],',
            '"data_prev":[', GROUP_CONCAT(unsubscribers ORDER BY week_num), '],',
            '"goals":[', GROUP_CONCAT(goal_subscribers ORDER BY week_num), ']',
        '}}',
        '}}'
    ) AS output_json
FROM ordered
GROUP BY site_id;

    """
    return visits_day_query,  visits_ytd_query, visits_month_query