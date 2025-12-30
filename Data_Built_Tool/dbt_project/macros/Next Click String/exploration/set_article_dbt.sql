 {% macro string_next_click_set_article_dbt() %}      
        
        {% set site = var('site')%}
        {% set historic = var('exploration')['historic_data']%}
        {% set columns= var('exploration')['columns']%}
        CREATE  PROCEDURE `set_article_dbt_{{site}}`()
        BEGIN
        SELECT 
            JSON_OBJECT(
                'site', siteid,
                'article_id', id,
                'article_name', title,
                 {{columns}}
                'date_posted', date,
                'date_updated',  DATE(Modified),
                'url', link,
                'avatar', '',
                'status_text', '',
                'status_color', ''
            ) AS result
        FROM 
            prod.site_archive_post_string
        WHERE 
            siteid = {{site}} and 
            id is not null
             {% if historic %}
                 AND date >= DATE_SUB(CURDATE(), INTERVAL 3 DAY)
             {% endif %}
             ;
        END

{% endmacro %}