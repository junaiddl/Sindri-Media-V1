

def combine_queries():
    visit_day = f""" 
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
"""
    visit_month = """
WITH date_range AS (
    SELECT 
        DATE_SUB(MAX(date), INTERVAL 13 MONTH) AS start_date,
        MAX(date) AS end_date
    FROM pre_stage.subscriber_master_list
),

weekly_data AS (
    SELECT
        s.site_id,
        WEEK(s.date, 3) AS week_num,
        Sum(COALESCE(s.subscribed_count, 0)) AS new_subscribers,
        Sum(COALESCE(s.unsubscribed_count, 0)) AS unsubscribers,
        AVG(COALESCE(s.subscriber_count, 0)) AS goal_subscribers
    FROM pre_stage.subscriber_master_list s
    JOIN date_range r
      ON s.date BETWEEN r.start_date AND r.end_date
    WHERE s.site_id = 14
    GROUP BY s.site_id, WEEK(s.date, 3)
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
            '"bars":[true,true,false],',
            '"categories":[', GROUP_CONCAT(week_num ORDER BY week_num), '],',
            '"data":[', GROUP_CONCAT(new_subscribers ORDER BY week_num), '],',
            '"data_prev":[', GROUP_CONCAT(unsubscribers ORDER BY week_num), '],',
            '"goals":[', GROUP_CONCAT(goal_subscribers ORDER BY week_num), ']',
        '}',
        '}'
    ) AS output_json
FROM ordered
GROUP BY site_id;
"""
    visit_ytd = """
WITH date_range AS (
    SELECT 
        DATE_SUB(MAX(date), INTERVAL 13 MONTH) AS start_date,
        MAX(date) AS end_date
    FROM pre_stage.subscriber_master_list
),

monthly_data AS (
    SELECT
        s.site_id,
        MONTH(s.date) AS month,
        DATE_FORMAT(s.date, '%b') AS monthname,
        sum(COALESCE(s.subscribed_count, 0)) AS new_subscribers,
        sum(COALESCE(s.unsubscribed_count, 0)) AS unsubscribers,
        AVG(COALESCE(s.subscriber_count, 0)) AS goal_subscribers
    FROM pre_stage.subscriber_master_list s
    JOIN date_range r
      ON s.date BETWEEN r.start_date AND r.end_date
    WHERE s.site_id = 14
    GROUP BY s.site_id, MONTH(s.date), DATE_FORMAT(s.date, '%b')
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
            '"bars":[true,true,false],',
            '"categories":[', GROUP_CONCAT(CONCAT('"', monthname, '"') ORDER BY month), '],',
            '"data":[', GROUP_CONCAT(new_subscribers ORDER BY month), '],',
            '"data_prev":[', GROUP_CONCAT(unsubscribers ORDER BY month), '],',
            '"goals":[', GROUP_CONCAT(goal_subscribers ORDER BY month), ']',
        '}',
        '}'
    ) AS output_json
FROM ordered
GROUP BY site_id;
"""
    return visit_day,  visit_month,visit_ytd